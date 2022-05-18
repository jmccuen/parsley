package com.thingworx.resources.parsley;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.XML;
import org.slf4j.Logger;
import org.w3c.dom.Document;

import com.monitorjbl.xlsx.StreamingReader;
import com.thingworx.common.RESTAPIConstants;
import com.thingworx.common.exceptions.InvalidRequestException;
import com.thingworx.common.utils.DateUtilities;
import com.thingworx.data.util.InfoTableInstanceFactory;
import com.thingworx.entities.utils.ThingUtilities;
import com.thingworx.logging.LogUtilities;
import com.thingworx.metadata.FieldDefinition;
import com.thingworx.metadata.annotations.ThingworxServiceDefinition;
import com.thingworx.metadata.annotations.ThingworxServiceParameter;
import com.thingworx.metadata.annotations.ThingworxServiceResult;
import com.thingworx.resources.Resource;
import com.thingworx.things.Thing;
import com.thingworx.things.repository.FileRepositoryThing;
import com.thingworx.types.BaseTypes;
import com.thingworx.types.InfoTable;
import com.thingworx.types.collections.ValueCollection;
import com.thingworx.types.primitives.structs.Location;

public class Parsley extends Resource {

	private static final long serialVersionUID = 1L;
	protected final static Logger _logger = LogUtilities.getInstance().getApplicationLogger(Parsley.class);

	// set the default date format
	private String _dateFormat = "RAW";
	private Boolean _hasDatashape = false;
	private Boolean _hasHeader = false;
	private String _customHeaders;

	// This is the value for which, if no data shape is passed in, Parsley will
	// assume a number value is a date when parsing JSON
	private Long _minDateMilliseconds = 946598400000L;

	@ThingworxServiceDefinition(name = "ParseJSON", description = "Parse JSON")
	@ThingworxServiceResult(name = "result", description = "Result", baseType = "INFOTABLE")
	public InfoTable ParseJSON(
			@ThingworxServiceParameter(name = "json", description = "JSON data to parse", baseType = "JSON") JSONObject json,
			@ThingworxServiceParameter(name = "dataShape", description = "Data shape", baseType = "DATASHAPENAME", aspects = {
					"defaultValue:" }) String dataShape,
			@ThingworxServiceParameter(name = "dateFormat", description = "joda format - e.g. 'yyyy-MM-dd'T'HH:mm:ss.SSSZ' ", baseType = "STRING", aspects = {
					"defaultValue:RAW" }) String dateFormat,
			@ThingworxServiceParameter(name = "minDateMilliseconds", description = "i.e. 10000000000;  only used if dateFormat is undefined or RAW", baseType = "LONG", aspects = {
					"defaultValue:100000000" }) Long minDateMilliseconds)
			throws Exception {

		_logger.trace("Entering Service: ParseJSON");

		// TODO: this should really check to make sure the class is a JSON Array or a
		// JSON Object
		if (json == null) {
			throw new InvalidRequestException("JSON Object must be specified",
					RESTAPIConstants.StatusCode.STATUS_NOT_ACCEPTABLE);
		}
		if (!minDateMilliseconds.equals(null) && minDateMilliseconds > 0) {
			_minDateMilliseconds = minDateMilliseconds;
		}
		if (dateFormat != null && !dateFormat.isEmpty()) {
			_dateFormat = dateFormat;
		}

		// this tracks the ordinal of the fields in the datashape; this is only used if
		// no datashape is passed in
		int ord = 0;
		InfoTable it;
		Boolean hasShape = false;

		if (dataShape == null || dataShape.isEmpty()) {
			it = new InfoTable();
		} else {
			hasShape = true;
			try {
				it = InfoTableInstanceFactory.createInfoTableFromDataShape(dataShape);
			} catch (Exception e) {
				throw new InvalidRequestException("DataShape not found",
						RESTAPIConstants.StatusCode.STATUS_NOT_ACCEPTABLE);
			}
			;
		}
		;

		// this is the equivalent of an InfoTable row item
		JSONObject values = new JSONObject();
		Iterator<?> keys = json.keys();

		while (keys.hasNext()) {

			String key = (String) keys.next();
			Object value = json.get(key);
			String fieldShape = null;

			// only create a new field if one hasnt already been added to the infotable for
			// this field
			try {
				fieldShape = it.getField(key).getDataShapeName();
			} catch (Exception e) {
			}
			BaseTypes baseType = BaseTypes.STRING;
			if (!hasShape || !it.hasField(key)) {
				baseType = getType(value);
				FieldDefinition field = new FieldDefinition();
				field.setName(key);
				field.setOrdinal(ord);
				field.setBaseType(baseType);
				it.addField(field);
			}
			ord++;

			// if I am not able to parse a JSON value at all, return an error for that field
			// this should really never happen, if it does something went horribly wrong
			try {
				value = parseJSONValue(value, fieldShape);
			} catch (Exception e) {
				it.getField(key).setBaseType(BaseTypes.STRING);
				((JSONObject) value).put("ERROR", BaseTypes.STRING);
			}
			// if the value was parsed correctly, but bombs out when converting it to the
			// given base type for the field
			// this will retroactively change the base type on the dataShape to a string
			// this usually happens if the first value was a number and a later value for
			// the same field is a string
			try {
				values.put(key, BaseTypes.ConvertToPrimitive(value, baseType));
			} catch (Exception e) {
				it.getField(key).setBaseType(BaseTypes.STRING);
				values.put(key, BaseTypes.ConvertToPrimitive(value, BaseTypes.STRING));
			}
		}
		it.AddRow(values);
		return it;

	}

	@ThingworxServiceDefinition(name = "ParseCSV", description = "Parse CSV file from a repository")
	@ThingworxServiceResult(name = "result", description = "Result", baseType = "INFOTABLE")
	public InfoTable ParseCSV(
			@ThingworxServiceParameter(name = "fileRepository", description = "File repository name", baseType = "THINGNAME") String fileRepository,
			@ThingworxServiceParameter(name = "path", description = "Path to file", baseType = "STRING", aspects = {
					"defaultValue:/" }) String path,
			@ThingworxServiceParameter(name = "dataShape", description = "Data shape", baseType = "DATASHAPENAME") String dataShape,
			@ThingworxServiceParameter(name = "hasHeader", description = "File has header row", baseType = "BOOLEAN", aspects = {
					"defaultValue:false" }) Boolean hasHeader,
			@ThingworxServiceParameter(name = "columnMappings", description = "Column maps", baseType = "STRING") String columnMappings,
			@ThingworxServiceParameter(name = "latitudeField", description = "Latitude field index", baseType = "NUMBER") Double latitudeField,
			@ThingworxServiceParameter(name = "longitudeField", description = "Longitude field index", baseType = "NUMBER") Double longitudeField,
			@ThingworxServiceParameter(name = "dateFormat", description = "Date format", baseType = "STRING") String dateFormat,
			@ThingworxServiceParameter(name = "fieldDelimiter", description = "Field delimiter", baseType = "STRING", aspects = {
					"defaultValue:," }) String fieldDelimiter,
			@ThingworxServiceParameter(name = "stringDelimiter", description = "String value delimiter", baseType = "STRING", aspects = {
					"defaultValue:\"" }) String stringDelimiter,
			@ThingworxServiceParameter(name = "customFieldNames", description = "Comma sperated list of field names to use if there is no datashape", baseType = "STRING") String customHeaders)
			throws Exception {

		_hasHeader = hasHeader;
		_dateFormat = dateFormat;
		_customHeaders = customHeaders;

		if (!(fileRepository != null && !fileRepository.isEmpty())) {
			throw new InvalidRequestException("File Repository Must Be Specified",
					RESTAPIConstants.StatusCode.STATUS_NOT_ACCEPTABLE);
		}

		Thing thing = ThingUtilities.findThing(fileRepository);
		if (thing == null) {
			throw new InvalidRequestException("File Repository [" + fileRepository + "] Does Not Exist",
					RESTAPIConstants.StatusCode.STATUS_NOT_FOUND);
		}
		if (!(thing instanceof FileRepositoryThing)) {
			throw new InvalidRequestException("Thing [" + fileRepository + "] Is Not A File Repository",
					RESTAPIConstants.StatusCode.STATUS_NOT_FOUND);
		}
		FileRepositoryThing repo = (FileRepositoryThing) thing;
		InfoTable it = new InfoTable();

		if (!(dataShape != null && !dataShape.isEmpty())) {
			_hasDatashape = false;
		} else {
			_hasDatashape = true;
			it = InfoTableInstanceFactory.createInfoTableFromDataShape(dataShape);
		}
		;
		InputStreamReader reader;
		try {
			reader = new InputStreamReader(repo.openFileForRead(path));
		} catch (Exception eOpen) {
			throw new InvalidRequestException(
					"Unable To Open [" + path + "] in [" + fileRepository + "] : " + eOpen.getMessage(),
					RESTAPIConstants.StatusCode.STATUS_NOT_FOUND);
		}
		try {
			parseFromReader(reader, it, columnMappings, hasHeader, fieldDelimiter, stringDelimiter, latitudeField,
					longitudeField, dateFormat);
		} catch (IndexOutOfBoundsException e) {
			try {
				reader.close();
			} catch (Exception eClose) {

			}
			;
			throw new IndexOutOfBoundsException(
					"Array index was out of bounds. This generally happens when either the number of columns do not match up to those provided by the data shape."
							+ " It can also occur if there was an error parsing a datetime based on the input format. - "
							+ e.getMessage());
		}
		return it;
	}

	private void setFieldType(InfoTable it, ArrayList<String> fieldValues, HashMap<String, Integer> fieldIndices)
			throws Exception {
		// read row values and set infotable appropriately

		for (FieldDefinition fieldDefinition : it.getDataShape().getFields().values()) {
			int colIndex = ((Integer) fieldIndices.get(fieldDefinition.getName())).intValue();
			if (colIndex >= 0) {
				// if we have a variant that means it hasnt been set yet
				if (it.getField(fieldDefinition.getName()).getBaseType() == BaseTypes.VARIANT) {
					it.getField(fieldDefinition.getName()).setBaseType(getTypeFromString(fieldValues.get(colIndex)));
				} else if (it.getField(fieldDefinition.getName())
						.getBaseType() != getTypeFromString(fieldValues.get(colIndex))) {
					it.getField(fieldDefinition.getName()).setBaseType(BaseTypes.STRING);
				}
			}
		}

	}

	protected void parseFromReader(Reader reader, InfoTable it, String columnMappings, Boolean hasHeader,
			String fieldDelimiter, String stringDelimiter, Double latitudeField, Double longitudeField,
			String dateFormat) throws Exception {

		// this really only matters if they have a data shape and a column mapping
		String[] mappedColumns = new String[0];
		if (columnMappings != null && !columnMappings.isEmpty()) {
			mappedColumns = columnMappings.split(";");
		} else {
			ArrayList<FieldDefinition> orderedFields = it.getDataShape().getFields().getOrderedFieldsByOrdinal();
			int nFields = orderedFields.size();

			mappedColumns = new String[nFields];
			for (int i = 0; i < nFields; i++) {
				mappedColumns[i] = ((FieldDefinition) orderedFields.get(i)).getName();
			}
		}
		HashMap<String, Integer> columnIndices = new HashMap<String, Integer>();

		int col = 0;
		for (FieldDefinition fieldDefinition : it.getDataShape().getFields().values()) {
			int colIndex = -1;
			for (col = 0; col < mappedColumns.length; col++) {
				String columnName = mappedColumns[col];
				if (columnName.equals(fieldDefinition.getName())) {
					colIndex = col;
					break;
				}
			}
			columnIndices.put(fieldDefinition.getName(), Integer.valueOf(colIndex));
		}
		int latitudeColIndex = -1;
		if (latitudeField != null) {
			latitudeColIndex = latitudeField.intValue();
		}
		int longitudeColIndex = -1;
		if (longitudeField != null) {
			longitudeColIndex = longitudeField.intValue();
		}
		int fieldChar = fieldDelimiter.charAt(0);
		int quoteChar = stringDelimiter.charAt(0);

		BufferedReader br = new BufferedReader(reader);
		int charRead = br.read();

		StringBuilder currentFieldValue = new StringBuilder();

		int rowNumber = 0;
		ArrayList<String> fieldValues = new ArrayList<String>();
		// loop through each character and append to the current field value until you
		// find the next delimiter
		while (charRead != -1) {
			if (charRead == quoteChar) {
				boolean done = false;
				while (!done) {
					charRead = br.read();
					if (charRead == -1) {
						throw new Exception("Unexpected end of file while parsing CSV input");
					}
					if (charRead == quoteChar) {
						br.mark(1);
						charRead = br.read();
						if (charRead == quoteChar) {
							currentFieldValue.append((char) quoteChar);
						} else {
							if (charRead != -1) {
								br.reset();
							}
							done = true;
						}
					} else {
						currentFieldValue.append((char) charRead);
					}
				}
			} else if (charRead == fieldChar) {
				fieldValues.add(currentFieldValue.toString());
				currentFieldValue.setLength(0);
			} else if (charRead == 10) {
				br.mark(1);
				charRead = br.read();
				if ((charRead != -1) && (charRead != 13)) {
					br.reset();
				}
				fieldValues.add(currentFieldValue.toString());
				currentFieldValue.setLength(0);

				// have a row here
				processFieldSet(it, fieldValues, columnIndices, latitudeColIndex, longitudeColIndex, dateFormat,
						rowNumber);
				rowNumber++;

				fieldValues.clear();
			} else if (charRead == 13) {
				br.mark(1);
				charRead = br.read();
				if ((charRead != -1) && (charRead != 10)) {
					br.reset();
				}
				fieldValues.add(currentFieldValue.toString());
				currentFieldValue.setLength(0);
				processFieldSet(it, fieldValues, columnIndices, latitudeColIndex, longitudeColIndex, dateFormat,
						rowNumber);
				rowNumber++;

				fieldValues.clear();
			} else {
				currentFieldValue.append((char) charRead);
			}
			charRead = br.read();
		}

		// make sure to add the remaining field value if there was no newline character
		// at the end of the file
		if (currentFieldValue.length() > 0) {
			fieldValues.add(currentFieldValue.toString());
		}
		;

		if (fieldValues.size() > 0) {
			processFieldSet(it, fieldValues, columnIndices, latitudeColIndex, longitudeColIndex, dateFormat, rowNumber);
			rowNumber++;
		}
		try {
			reader.close();
		} catch (Exception eClose) {
		}
	}

	protected void processFieldSet(InfoTable it, ArrayList<String> fieldValues, HashMap<String, Integer> fieldIndices,
			int latitudeField, int longitudeField, String dateFormat, int rowNumber) throws Exception {
		if (rowNumber == 0 && !_hasDatashape) {
			if (_customHeaders == null || _customHeaders.isEmpty()) {
				if (!_hasHeader) {
					try {
						// need to make our own headers if there are none and no custom headers were
						// passed in
						for (int i = 0; i < fieldValues.size(); i++) {
							FieldDefinition field = new FieldDefinition();
							field.setBaseType(BaseTypes.VARIANT);
							field.setName("Value" + (i + 1));
							field.setOrdinal(i);
							field.setDescription("");
							it.addField(field);

							// we dont have a field map because there are no fields to map
							// but we still need this index to call setFieldType later
							fieldIndices.put("Value" + (i + 1), i);
						}
						// if we're on row 0 and we dont have headers, lets set out field types if we
						// dont have a data shape
						setFieldType(it, fieldValues, fieldIndices);
					} catch (Exception e) {
						throw new InvalidRequestException(
								"Error creating value headers for data shape - " + e.getMessage(),
								RESTAPIConstants.StatusCode.STATUS_NOT_ACCEPTABLE);
					}
				} else {
					// use the headers if theyre there and no custom headers were passed in
					// we need to replace some characters so we can create a datashape from them
					try {
						for (int i = 0; i < fieldValues.size(); i++) {
							String name = fieldValues.get(i);

							// get rid of any weird null characters, strings, parens, which arent allowed in
							// property names
							name = name.replaceAll("[\uFEFF-\uFFFF]", "");
							name = name.replaceAll("\\s+", "");
							name = name.replaceAll("[(]", "_");
							name = name.replaceAll("[)]", "");

							// cant start with a number
							if (Character.isDigit(name.charAt(0))) {
								name = "_" + name;
							}

							FieldDefinition field = new FieldDefinition();
							field.setBaseType(BaseTypes.VARIANT);
							field.setName(name);
							field.setOrdinal(i);
							field.setDescription("");
							it.addField(field);

							fieldIndices.put(name, i);
						}
					} catch (Exception e) {
						throw new InvalidRequestException("Error parsing headers into DataShape - " + e.getMessage(),
								RESTAPIConstants.StatusCode.STATUS_NOT_ACCEPTABLE);

					}
				}
			} else {
				// use custom headers
				try {
					String[] headers = _customHeaders.split(",");
					int i = 0;
					for (String header : headers) {
						FieldDefinition field = new FieldDefinition();
						field.setBaseType(BaseTypes.VARIANT);
						field.setName(header);
						field.setOrdinal(i);
						field.setDescription("");
						it.addField(field);

						fieldIndices.put(header, i);
						i++;
					}
				} catch (Exception e) {
					throw new InvalidRequestException("Error parsing custom headers into DataShape - " + e.getMessage(),
							RESTAPIConstants.StatusCode.STATUS_NOT_ACCEPTABLE);
				}
			}
		} else if (rowNumber == 1 && _hasHeader && !_hasDatashape) {
			setFieldType(it, fieldValues, fieldIndices);
		}

		if (rowNumber != 0 || _hasHeader == false) {
			// parse row (field set) into the correct infotable row and append
			ValueCollection values = new ValueCollection();
			for (FieldDefinition fieldDefinition : it.getDataShape().getFields().values()) {
				int colIndex = ((Integer) fieldIndices.get(fieldDefinition.getName())).intValue();
				if (colIndex >= 0) {
					try {
						switch (fieldDefinition.getBaseType()) {
						case DATETIME:
							String dateValue = (String) fieldValues.get(colIndex);
							if (dateValue != null && !dateValue.isEmpty()) {
								if (dateFormat != null) {
									values.put(fieldDefinition.getName(),
											BaseTypes.ConvertToPrimitive(
													DateUtilities.parseDateTime(dateValue, dateFormat),
													fieldDefinition.getBaseType()));
								} else {
									values.put(fieldDefinition.getName(),
											BaseTypes.ConvertToPrimitive(dateValue, fieldDefinition.getBaseType()));
								}
							}
							break;
						case NUMBER:
							String numberValue = (String) fieldValues.get(colIndex);
							if (numberValue != null && !numberValue.isEmpty()) {
								values.put(fieldDefinition.getName(),
										BaseTypes.ConvertToPrimitive(numberValue, fieldDefinition.getBaseType()));
							}
							break;
						case LOCATION:
							if ((latitudeField != -1) && (longitudeField != -1)) {
								String latitudeValue = (String) fieldValues.get(latitudeField);
								String longitudeValue = (String) fieldValues.get(longitudeField);
								if ((latitudeValue != null && !latitudeValue.isEmpty())
										&& (longitudeValue != null && !longitudeValue.isEmpty())) {
									values.put(fieldDefinition.getName(), BaseTypes.ConvertToPrimitive(
											latitudeValue + "," + longitudeValue, fieldDefinition.getBaseType()));
								}
							} else {
								String value = (String) fieldValues.get(colIndex);
								if (value != null && !value.isEmpty()) {

									// check that its really a location
									if (value.matches(
											"^[-+]?([1-8]?\\d(\\.\\d+)?|90(\\.0+)?),\\s*[-+]?(180(\\.0+)?|((1[0-7]\\d)|([1-9]?\\d))(\\.\\d+)?)$")) {
										values.put(fieldDefinition.getName(),
												BaseTypes.ConvertToPrimitive(value, fieldDefinition.getBaseType()));
									} else {
										if (!_hasDatashape) {
											// need to fix the old values which as now json objects
											for (ValueCollection row : it.getRows()) {
												Location location = (Location) row.getValue(fieldDefinition.getName());
												String stringLocation = location.getLatitude() + ","
														+ location.getLongitude();
												row.SetStringValue(fieldDefinition.getName(), stringLocation);
											}

											it.getField(fieldDefinition.getName()).setBaseType(BaseTypes.STRING);
											values.put(fieldDefinition.getName(),
													BaseTypes.ConvertToPrimitive(value, fieldDefinition.getBaseType()));
										} else {
											throw new InvalidRequestException(
													"Error parsing location for " + fieldDefinition.getName()
															+ " - at row  " + rowNumber,
													RESTAPIConstants.StatusCode.STATUS_NOT_ACCEPTABLE);
										}
									}
								}
							}
							break;
						case INTEGER:
							String integerValue = (String) fieldValues.get(colIndex);
							if (integerValue != null && !integerValue.isEmpty()) {
								// are we really an int, or are we a double?
								try {
									Integer.parseInt(integerValue);
									values.put(fieldDefinition.getName(),
											BaseTypes.ConvertToPrimitive(integerValue, fieldDefinition.getBaseType()));
								} catch (Exception e) {
									if (!_hasDatashape) {
										// its actually a number..
										it.getField(fieldDefinition.getName()).setBaseType(BaseTypes.NUMBER);
										values.put(fieldDefinition.getName(), BaseTypes.ConvertToPrimitive(integerValue,
												fieldDefinition.getBaseType()));
									} else {
										throw new InvalidRequestException(
												"Error parsing integer for " + fieldDefinition.getName() + " - at row  "
														+ rowNumber + " - " + e.getMessage(),
												RESTAPIConstants.StatusCode.STATUS_NOT_ACCEPTABLE);
									}
								}
							}
							break;
						case BOOLEAN:
							String boolValue = (String) fieldValues.get(colIndex);
							if (boolValue != null) {
								if (!_hasDatashape) {
									// make sure this is really a boolean
									if (boolValue.equals("true") || boolValue.equals("false")) {
										values.put(fieldDefinition.getName(),
												BaseTypes.ConvertToPrimitive(boolValue, BaseTypes.STRING));
									} else {
										it.getField(fieldDefinition.getName()).setBaseType(BaseTypes.STRING);
										values.put(fieldDefinition.getName(),
												BaseTypes.ConvertToPrimitive(boolValue, BaseTypes.STRING));
									}
								} else {
									values.put(fieldDefinition.getName(),
											BaseTypes.ConvertToPrimitive(boolValue, BaseTypes.STRING));
								}
							}
							break;
						default:
							String value = (String) fieldValues.get(colIndex);
							if (value != null) {
								values.put(fieldDefinition.getName(),
										BaseTypes.ConvertToPrimitive(value, fieldDefinition.getBaseType()));
							}
							break;
						}
					} catch (Exception e) {
						if (!_hasDatashape) {
							// if i hit this it must be a type mismatch, lets see if we can add it as
							// whatever type it is..
							if (fieldDefinition.getBaseType() == BaseTypes.DATETIME) {
								// need to fix the old values which weve already converted, we need to convert
								// them back to the originals
								// this is likely really slow, but it makes sure that we give the correct output
								// when this issue occurs
								// eventually this will be refactored to scan the csv once to determin the types
								// then write to the infotable, but this will work for now
								for (ValueCollection row : it.getRows()) {
									DateTime date = (DateTime) row.getValue(fieldDefinition.getName());
									String stringDate = DateUtilities.formatDate(date, dateFormat);
									row.SetStringValue(fieldDefinition.getName(), stringDate);
								}
							}
							String value = (String) fieldValues.get(colIndex);
							if (value != null) {
								it.getField(fieldDefinition.getName()).setBaseType(getTypeFromString(value));
								values.put(fieldDefinition.getName(),
										BaseTypes.ConvertToPrimitive(value, getTypeFromString(value)));
							}
						} else {
							throw new InvalidRequestException(
									"Error parsing value for " + fieldDefinition.getName() + " - at row  " + rowNumber
											+ " - " + e.getMessage(),
									RESTAPIConstants.StatusCode.STATUS_NOT_ACCEPTABLE);
						}
					}
				} else if ((fieldDefinition.getBaseType() == BaseTypes.LOCATION) && (latitudeField != -1)
						&& (longitudeField != -1)) {
					String latitudeValue = (String) fieldValues.get(latitudeField);
					String longitudeValue = (String) fieldValues.get(longitudeField);
					if ((latitudeValue != null && !latitudeValue.isEmpty())
							&& (longitudeValue != null && !longitudeValue.isEmpty())) {
						values.put(fieldDefinition.getName(), BaseTypes.ConvertToPrimitive(
								latitudeValue + "," + longitudeValue, fieldDefinition.getBaseType()));
					}
				}
			}
			it.addRow(values);
		}
	}

	protected Object parseJSONValue(Object value, String fieldShape) {
		// check to see if this is a JSONN object or JSON array. If it is a JSON object
		// this makes a recursive call to parse JSON using the current value as the
		// input
		// if this is an array we create a new infotable object with a value field and
		// add each of items
		try {
			if (value instanceof JSONObject) {
				InfoTable result = ParseJSON((JSONObject) value, fieldShape, _dateFormat, _minDateMilliseconds);
				return result;
			} else if (value instanceof JSONArray) {
				InfoTable result = new InfoTable();
				try {
					result = InfoTableInstanceFactory.createInfoTableFromDataShape(fieldShape);
				} catch (Exception e) {
				}
				for (int i = 0; i < ((JSONArray) value).length(); i++) {
					Object item = ((JSONArray) value).get(i);
					if (item instanceof JSONObject || item instanceof JSONArray) {
						InfoTable itemTable = ParseJSON((JSONObject) item, fieldShape, _dateFormat,
								_minDateMilliseconds);
						ValueCollection values = itemTable.getRow(0);
						result.setDataShape(itemTable.getDataShape());
						result.addRow(values);
					} else {
						// only add a new field if this is the first row
						if (i == 0) {
							FieldDefinition definition = new FieldDefinition();
							definition.setName("values");
							definition.setOrdinal(i);
							definition.setBaseType(getType(item));
							result.addField(definition);
						}
						ValueCollection values = new ValueCollection();
						values.put("values", BaseTypes.ConvertToPrimitive(item, getType(item)));
						result.addRow(values);
					}
				}
				return result;

			} else if (IsDate(value)) {
				// TODO: check if they pass in a dateShape and if the field type is not a date
				// don't attempt to parse
				try {
					// if it's a date type, parse it using the passed in date format. If it's a RAW
					// json date, it comparsed the number value against the mindate milliseconds
					if (_dateFormat == "RAW") {
						Double v = new Double(Double.parseDouble(value.toString()));
						if (v % 1 == 0 && v >= _minDateMilliseconds) {
							return BaseTypes.ConvertToPrimitive(new Date(v.longValue()), BaseTypes.DATETIME);
						}
						;
					} else {
						return BaseTypes.ConvertToPrimitive(DateUtilities.parseDateTime((String) value, _dateFormat),
								BaseTypes.DATETIME);
					}
				} catch (Exception e) {
					throw new Exception("Unable to parse date with provided format -- " + e.getMessage());
				}
			}
			// { value =
			// BaseTypes.ConvertToPrimitive(DateUtilities.parseDateTime((new
			// DateTime(value).toDateTimeISO()).toString()),
			// BaseTypes.DATETIME); }
			// catch (Exception e) {}

		} catch (Exception e) {
			_logger.error(e.getMessage());
			return null;
		}
		return value;
	}

	protected BaseTypes getTypeFromString(String value) {
		BaseTypes result = BaseTypes.STRING;

		try {
			if (IsDate(value)) {
				result = BaseTypes.DATETIME;
			} else if (value.matches("[-+]?\\d*\\.?\\d+")) {
				// is it an int or a double?
				try {
					Integer.parseInt(value);
					result = BaseTypes.INTEGER;
				} catch (Exception e) {
					// must be a double
					result = BaseTypes.NUMBER;
				}
			} else if (value.matches(
					"^[-+]?([1-8]?\\d(\\.\\d+)?|90(\\.0+)?),\\s*[-+]?(180(\\.0+)?|((1[0-7]\\d)|([1-9]?\\d))(\\.\\d+)?)$")) {
				result = BaseTypes.LOCATION;
			} else if (value.equals("true") || value.equals("false")) {
				result = BaseTypes.BOOLEAN;
			}
		} catch (Exception e) {
			return result;
		}
		return result;
	}

	protected BaseTypes getType(Object value) {
		BaseTypes result;

		try {
			String cname = value.getClass().getName();

			if (value instanceof JSONObject || value instanceof JSONArray) {
				result = BaseTypes.INFOTABLE;
			} else if (IsDate(value)) {
				result = BaseTypes.DATETIME;
			} else if (cname == Boolean.class.getName()) {
				result = BaseTypes.BOOLEAN;
			} else if (cname == Float.class.getName() || cname == Double.class.getName()
					|| cname == Integer.class.getName() || cname == Long.class.getName()) {
				result = BaseTypes.NUMBER;
			} else {
				result = BaseTypes.STRING;
			}
		} catch (Exception e) {
			result = BaseTypes.STRING;
		}
		return result;
	}

	protected Boolean IsDate(Object value) {
		try {
			if (_dateFormat == "RAW") {
				Double v = new Double(Double.parseDouble(value.toString()));
				if (v % 1 == 0 && v >= _minDateMilliseconds) {
					new Date(v.longValue());
				} else {
					return false;
				}
			} else {
				DateUtilities.parseDateTime((String) value, _dateFormat);
			}
			;
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	@SuppressWarnings("deprecation")
	@ThingworxServiceDefinition(name = "ParseXLSX", description = "", category = "", isAllowOverride = false, aspects = {
			"isAsync:false" })
	@ThingworxServiceResult(name = "result", description = "", baseType = "INFOTABLE", aspects = {
			"isEntityDataShape:true" })
	public InfoTable ParseXLSX(
			@ThingworxServiceParameter(name = "path", description = "", baseType = "STRING") String path,
			@ThingworxServiceParameter(name = "fileRepository", description = "", baseType = "THINGNAME") String fileRepository,
			@ThingworxServiceParameter(name = "hasHeader", description = "", baseType = "BOOLEAN", aspects = {
					"defaultValue:false" }) Boolean hasHeader,
			@ThingworxServiceParameter(name = "sheetName", description = "", baseType = "STRING") String sheetName,
			@ThingworxServiceParameter(name = "dateFormat", description = "", baseType = "STRING") String dateFormat,
			@ThingworxServiceParameter(name = "dataShape", description = "", baseType = "DATASHAPENAME") String dataShape,
			@ThingworxServiceParameter(name = "rowCacheSize", description = "Number of rows to cache in the stream reader", baseType = "INTEGER", aspects = {
					"defaultValue:100" }) Integer rowCacheSize,
			@ThingworxServiceParameter(name = "streamBufferSize", description = "Buffer size of the stream reader", baseType = "INTEGER", aspects = {
					"defaultValue:4096" }) Integer streamBufferSize)
			throws Exception {

		InfoTable it = new InfoTable();

		_hasHeader = hasHeader;
		_dateFormat = dateFormat;

		if (!(fileRepository != null && !fileRepository.isEmpty())) {
			throw new InvalidRequestException("File Repository Must Be Specified",
					RESTAPIConstants.StatusCode.STATUS_NOT_ACCEPTABLE);
		}

		Thing thing = ThingUtilities.findThing(fileRepository);
		if (thing == null) {
			throw new InvalidRequestException("File Repository [" + fileRepository + "] Does Not Exist",
					RESTAPIConstants.StatusCode.STATUS_NOT_FOUND);
		}
		if (!(thing instanceof FileRepositoryThing)) {
			throw new InvalidRequestException("Thing [" + fileRepository + "] Is Not A File Repository",
					RESTAPIConstants.StatusCode.STATUS_NOT_FOUND);
		}
		FileRepositoryThing repo = (FileRepositoryThing) thing;

		if (!(dataShape != null && !dataShape.isEmpty())) {
			_hasDatashape = false;
		} else {
			_hasDatashape = true;
			it = InfoTableInstanceFactory.createInfoTableFromDataShape(dataShape);
		}
		;

		FileInputStream excelFile;
		try {
			excelFile = repo.openFileForRead(path);
		} catch (Exception eOpen) {
			throw new InvalidRequestException(
					"Unable To Open [" + path + "] in [" + fileRepository + "] : " + eOpen.getMessage(),
					RESTAPIConstants.StatusCode.STATUS_NOT_FOUND);
		}

		Workbook workbook;
		try {
			workbook = StreamingReader.builder().rowCacheSize(rowCacheSize).bufferSize(streamBufferSize)
					.open(excelFile);
		} catch (Exception eOpen) {
			excelFile.close();
			throw new InvalidRequestException("Unable To Open [" + path + "] in [" + fileRepository
					+ "] -- invalid XLSX file : " + eOpen.getMessage(),
					RESTAPIConstants.StatusCode.STATUS_INTERNAL_ERROR);
		}

		Sheet sheet;
		try {
			sheet = workbook.getSheet(sheetName);
		} catch (Exception eOpen) {

			excelFile.close();
			throw new InvalidRequestException("Unable To Open [" + sheetName + "] in [" + path
					+ "] -- invalid Sheet Name : " + eOpen.getMessage(),
					RESTAPIConstants.StatusCode.STATUS_INTERNAL_ERROR);
		}

		Iterator<Row> iterator = sheet.iterator();

		Integer r = 0;
		Integer c = 0;

		while (iterator.hasNext()) {

			Row currentRow = iterator.next();
			Iterator<Cell> cellIterator = currentRow.iterator();
			c = 0;
			JSONObject values = new JSONObject();
			while (cellIterator.hasNext()) {
				Cell currentCell = cellIterator.next();

				if (r == 0 && !_hasDatashape) {
					if (hasHeader) {
						Object nameObj = "Error" + (c + 1);
						switch (currentCell.getCellType()) {
						case Cell.CELL_TYPE_STRING:
							nameObj = currentCell.getStringCellValue();
							break;
						case Cell.CELL_TYPE_BOOLEAN:
							nameObj = currentCell.getBooleanCellValue();
							break;
						case Cell.CELL_TYPE_NUMERIC:
							nameObj = currentCell.getNumericCellValue();
							break;
						case Cell.CELL_TYPE_FORMULA:
							switch (currentCell.getCachedFormulaResultType()) {
							case Cell.CELL_TYPE_STRING:
								nameObj = currentCell.getStringCellValue();
								break;
							case Cell.CELL_TYPE_BOOLEAN:
								nameObj = currentCell.getBooleanCellValue();
								break;
							case Cell.CELL_TYPE_NUMERIC:
								nameObj = currentCell.getNumericCellValue();
								break;
							}
							break;
						}
						String name = String.valueOf(nameObj);

						// get rid of any weird null characters, strings, parens, which arent allowed in
						// property names
						name = name.replaceAll("[\uFEFF-\uFFFF]", "");
						name = name.replaceAll("\\s+", "");
						name = name.replaceAll("[(]", "_");
						name = name.replaceAll("[)]", "");

						// cant start with a number
						if (Character.isDigit(name.charAt(0))) {
							name = "_" + name;
						}

						FieldDefinition field = new FieldDefinition();
						field.setBaseType(BaseTypes.VARIANT);
						field.setName(name);
						field.setOrdinal(c);
						field.setDescription("");
						it.addField(field);
					} else {
						FieldDefinition field = new FieldDefinition();

						field.setBaseType(BaseTypes.VARIANT);
						field.setName("Value" + (c + 1));
						field.setOrdinal(c);
						field.setDescription("");
						it.addField(field);
					}
				}

				FieldDefinition field = it.getDataShape().getFields().getOrderedFieldsByOrdinal().get(c);

				Object value = "Error - unknown type";

				switch (currentCell.getCellType()) {
				case Cell.CELL_TYPE_STRING:
					value = currentCell.getStringCellValue();
					break;
				case Cell.CELL_TYPE_BOOLEAN:
					value = currentCell.getBooleanCellValue();
					break;
				case Cell.CELL_TYPE_NUMERIC:
					value = currentCell.getNumericCellValue();
					break;
				case Cell.CELL_TYPE_FORMULA:
					switch (currentCell.getCachedFormulaResultType()) {
					case Cell.CELL_TYPE_STRING:
						value = currentCell.getStringCellValue();
						break;
					case Cell.CELL_TYPE_BOOLEAN:
						value = currentCell.getBooleanCellValue();
						break;
					case Cell.CELL_TYPE_NUMERIC:
						value = currentCell.getNumericCellValue();
						break;
					}
					break;
				}

				BaseTypes type = getTypeFromString(String.valueOf(value));

				if (it.getField(field.getName()).getBaseType() != BaseTypes.STRING
						&& it.getField(field.getName()).getBaseType() != type) {
					it.getField(field.getName()).setBaseType(type);
				}

				try {
					values.put(field.getName(), BaseTypes.ConvertToPrimitive(value, type));
				} catch (Exception e) {
					values.put(field.getName(), BaseTypes.ConvertToPrimitive(value, BaseTypes.STRING));
				}

				c++;
			}

			if (!hasHeader || r > 0) {
				it.AddRow(values);
			}
			r++;
		}
		;

		excelFile.close();

		return it;

	}

	@ThingworxServiceDefinition(name = "ParseXML", description = "Parse JSON")
	@ThingworxServiceResult(name = "result", description = "Result", baseType = "INFOTABLE")
	public InfoTable ParseXML(
			@ThingworxServiceParameter(name = "xml", description = "XML data to parse", baseType = "XML") Document xml,
			@ThingworxServiceParameter(name = "dataShape", description = "Data shape", baseType = "DATASHAPENAME", aspects = {
					"defaultValue:" }) String dataShape,
			@ThingworxServiceParameter(name = "dateFormat", description = "joda format - e.g. 'yyyy-MM-dd'T'HH:mm:ss.SSSZ' ", baseType = "STRING", aspects = {
					"defaultValue:RAW" }) String dateFormat,
			@ThingworxServiceParameter(name = "minDateMilliseconds", description = "i.e. 10000000000;  only used if dateFormat is undefined or RAW", baseType = "LONG", aspects = {
					"defaultValue:100000000" }) Long minDateMilliseconds)
			throws Exception {

		InfoTable it = new InfoTable();

		DOMSource domSource = new DOMSource(xml);
		StringWriter writer = new StringWriter();
		StreamResult result = new StreamResult(writer);
		TransformerFactory tf = TransformerFactory.newInstance();
		Transformer transformer = tf.newTransformer();
		transformer.transform(domSource, result);

		JSONObject json = XML.toJSONObject(writer.toString());

		it = this.ParseJSON(json, dataShape, dateFormat, minDateMilliseconds);

		return it;

	}

}
