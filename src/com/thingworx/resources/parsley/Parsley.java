package com.thingworx.resources.parsley;

import com.thingworx.common.RESTAPIConstants;
import com.thingworx.common.exceptions.InvalidRequestException;
import com.thingworx.common.utils.DateUtilities;
import com.thingworx.data.util.InfoTableInstanceFactory;
import com.thingworx.datashape.DataShape;
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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

public class Parsley extends Resource {

	private static final long serialVersionUID = 1L;
	protected static Logger _logger = LogUtilities.getInstance().getApplicationLogger(Parsley.class);
	private String _dateFormat = "RAW";
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
		_logger.trace("Exiting Service: ParseJSON");

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

		JSONObject values = new JSONObject();
		Iterator<?> keys = json.keys();

		while (keys.hasNext()) {

			String key = (String) keys.next();
			Object value = json.get(key);
			String fieldShape = null;
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
			try {
				value = parseJSONValue(value, fieldShape);
			} catch (Exception e) {
				it.getField(key).setBaseType(BaseTypes.STRING);
				((JSONObject) value).put("ERROR", BaseTypes.STRING);
			}
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
					"defaultValue:\"" }) String stringDelimiter)
			throws Exception {

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
		InfoTable it;
		if (!(dataShape != null && !dataShape.isEmpty())) {
			// throw new InvalidRequestException("Data Shape Must Be Specified",
			// RESTAPIConstants.StatusCode.STATUS_NOT_ACCEPTABLE);
			//Need to figure out what the dataShape is
			if (hasHeader) {
				InputStreamReader reader;
				try {
					reader = new InputStreamReader(repo.openFileForRead(path));
				} catch (Exception eOpen) {
					throw new InvalidRequestException(
							"Unable To Open [" + path + "] in [" + fileRepository + "] : " + eOpen.getMessage(),
							RESTAPIConstants.StatusCode.STATUS_NOT_FOUND);
				}
				try {
					DataShape ds = getDataShapeCSV(reader,hasHeader,stringDelimiter,fieldDelimiter);
					it = InfoTableInstanceFactory.createInfoTableFromDataShape(ds.getDataShape());
				} catch (Exception e) {
					throw new InvalidRequestException("Error parsing headers into DataShape - " + e.getMessage(),
							RESTAPIConstants.StatusCode.STATUS_NOT_ACCEPTABLE);
				};
				
			} else {
				throw new InvalidRequestException("DataShape must be specified if there are no headers",
						RESTAPIConstants.StatusCode.STATUS_NOT_ACCEPTABLE);
			}
		} else {
			it = InfoTableInstanceFactory.createInfoTableFromDataShape(dataShape);
		};
			InputStreamReader reader;
		try {
			reader = new InputStreamReader(repo.openFileForRead(path));
		} catch (Exception eOpen) {
			throw new InvalidRequestException(
					"Unable To Open [" + path + "] in [" + fileRepository + "] : " + eOpen.getMessage(),
					RESTAPIConstants.StatusCode.STATUS_NOT_FOUND);
		}
		parseFromReader(reader, it, columnMappings, hasHeader, fieldDelimiter, stringDelimiter, latitudeField,
				longitudeField, dateFormat);

		return it;
	}

	private DataShape getDataShapeCSV(InputStreamReader reader, Boolean hasHeader, String quoteChar, String delimiter) throws Exception {
		// TODO Auto-generated method stub
		DataShape ds = new DataShape();
		BufferedReader br = new BufferedReader(reader);
		if (hasHeader) {
				String headers = br.readLine();
				String[] array = headers.split(delimiter);
				int i = 0;
				for (String header : array) {
					FieldDefinition field = new FieldDefinition();
					field.setBaseType(BaseTypes.STRING);
					field.setName(header);
					field.setOrdinal(i);
					field.setDescription("");
					ds.addFieldDefinition(field);
				}
				br.close();
			return ds;
			
		}
		return null;
	}

	protected void parseFromReader(Reader reader, InfoTable it, String columnMappings, Boolean hasHeader,
			String fieldDelimiter, String stringDelimiter, Double latitudeField, Double longitudeField,
			String dateFormat) throws Exception {
		boolean firstRow = true;

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

		ArrayList<String> fieldValues = new ArrayList<String>();
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
				if (firstRow) {
					if (!hasHeader.booleanValue()) {
						processFieldSet(it, fieldValues, columnIndices, latitudeColIndex, longitudeColIndex,
								dateFormat);
					}
					firstRow = false;
				} else {
					processFieldSet(it, fieldValues, columnIndices, latitudeColIndex, longitudeColIndex, dateFormat);
				}
				fieldValues.clear();
			} else if (charRead == 13) {
				br.mark(1);
				charRead = br.read();
				if ((charRead != -1) && (charRead != 10)) {
					br.reset();
				}
				fieldValues.add(currentFieldValue.toString());
				currentFieldValue.setLength(0);
				if (firstRow) {
					if (!hasHeader.booleanValue()) {
						processFieldSet(it, fieldValues, columnIndices, latitudeColIndex, longitudeColIndex,
								dateFormat);
					}
					firstRow = false;
				} else {
					processFieldSet(it, fieldValues, columnIndices, latitudeColIndex, longitudeColIndex, dateFormat);
				}
				fieldValues.clear();
			} else {
				currentFieldValue.append((char) charRead);
			}
			charRead = br.read();
		}

		if (currentFieldValue.length() > 0) {
			fieldValues.add(currentFieldValue.toString());
		}
		;

		if (fieldValues.size() > 0) {
			if (firstRow) {
				if (!hasHeader.booleanValue()) {
					processFieldSet(it, fieldValues, columnIndices, latitudeColIndex, longitudeColIndex, dateFormat);
				}
				firstRow = false;
			} else {
				processFieldSet(it, fieldValues, columnIndices, latitudeColIndex, longitudeColIndex, dateFormat);
			}
		}
		try {
			reader.close();
		} catch (Exception eClose) {
		}
	}

	protected void processFieldSet(InfoTable it, ArrayList<String> fieldValues, HashMap<String, Integer> fieldIndices,
			int latitudeField, int longitudeField, String dateFormat) throws Exception {
		ValueCollection values = new ValueCollection();
		for (FieldDefinition fieldDefinition : it.getDataShape().getFields().values()) {
			int colIndex = ((Integer) fieldIndices.get(fieldDefinition.getName())).intValue();
			if (colIndex >= 0) {
				switch (fieldDefinition.getBaseType()) {
				case DATETIME:
					String dateValue = (String) fieldValues.get(colIndex);
					if (dateValue != null && !dateValue.isEmpty()) {
						if (dateFormat != null) {
							values.put(fieldDefinition.getName(), BaseTypes.ConvertToPrimitive(
									DateUtilities.parseDateTime(dateValue, dateFormat), fieldDefinition.getBaseType()));
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
							values.put(fieldDefinition.getName(),
									BaseTypes.ConvertToPrimitive(value, fieldDefinition.getBaseType()));
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
			} else if ((fieldDefinition.getBaseType() == BaseTypes.LOCATION) && (latitudeField != -1)
					&& (longitudeField != -1)) {
				String latitudeValue = (String) fieldValues.get(latitudeField);
				String longitudeValue = (String) fieldValues.get(longitudeField);
				if ((latitudeValue != null && !latitudeValue.isEmpty())
						&& (longitudeValue != null && !longitudeValue.isEmpty())) {
					values.put(fieldDefinition.getName(), BaseTypes
							.ConvertToPrimitive(latitudeValue + "," + longitudeValue, fieldDefinition.getBaseType()));
				}
			}
		}
		it.addRow(values);

	}

	protected Object parseJSONValue(Object value, String fieldShape) {
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
						FieldDefinition definition = new FieldDefinition();
						definition.setName("value" + (i + 1));
						definition.setOrdinal(i);
						definition.setBaseType(getType(item));
						ValueCollection values = new ValueCollection();
						values.put("value" + (i + 1), BaseTypes.ConvertToPrimitive(item, getType(item)));
						result.addField(definition);
						result.addRow(values);
					}
				}
				return result;
			} else if (IsDate(value)) {
				try {
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

}
