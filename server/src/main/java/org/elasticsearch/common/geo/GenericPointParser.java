/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.geo;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentSubParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.common.Strings.collectionToDelimitedString;

/**
 * This class can parse points from XContentParser and supports several formats:
 * @param <T> type of point to produce (either geographic or cartesian)
 */
public abstract class GenericPointParser<T> {
    private static final String X_FIELD = "x";
    private static final String Y_FIELD = "y";
    private static final String Z_FIELD = "z";
    private static final String TYPE = "type";
    private static final String COORDINATES = "coordinates";
    private final Map<String, FieldParser<?>> fields;
    private final String mapType;
    private final String xField;
    private final String yField;

    private abstract static class FieldParser<T> {
        final String name;

        abstract T parseField(XContentSubParser subParser) throws IOException;

        private FieldParser(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return this.name;
        }
    }

    private static class StringFieldParser extends FieldParser<String> {

        private StringFieldParser(String name) {
            super(name);
        }

        @Override
        String parseField(XContentSubParser subParser) throws IOException {
            if (subParser.currentToken() == XContentParser.Token.VALUE_STRING) {
                return subParser.text();
            } else {
                throw new ElasticsearchParseException("[{}] must be a string", name);
            }
        }
    }

    private static class DoubleFieldParser extends FieldParser<Double> {
        final String description;

        private DoubleFieldParser(String name, String description) {
            super(name);
            this.description = switch (description) {
                case "lon" -> "longitude";
                case "lat" -> "latitude";
                default -> description;
            };
        }

        @Override
        Double parseField(XContentSubParser subParser) throws IOException {
            return parseValidDouble(subParser, description);
        }
    }

    private static class DoubleArrayFieldParser extends FieldParser<List<Double>> {

        private DoubleArrayFieldParser(String name) {
            super(name);
        }

        @Override
        List<Double> parseField(XContentSubParser subParser) throws IOException {
            if (subParser.currentToken() == XContentParser.Token.START_ARRAY) {
                ArrayList<Double> coordinates = new ArrayList<>();
                while (subParser.nextToken() != XContentParser.Token.END_ARRAY) {
                    coordinates.add(parseValidDouble(subParser, name));
                }
                return coordinates;
            } else {
                throw new ElasticsearchParseException("[{}] must be an array", name);
            }
        }
    }

    /**
     * Construct the parser with some configuration settings
     * @param mapType whether the parser is for 'geo_point' or 'point'
     * @param xField the name of the first coordinate when constructing points (either 'x' or 'lat')
     * @param yField the name of the second coordinate when constructing points (either 'y' or 'lon')
     */
    public GenericPointParser(String mapType, String xField, String yField) {
        this.mapType = mapType;
        this.xField = xField;
        this.yField = yField;
        fields = new LinkedHashMap<>();
        fields.put(xField, new DoubleFieldParser(X_FIELD, xField));
        fields.put(yField, new DoubleFieldParser(Y_FIELD, yField));
        fields.put(Z_FIELD, new DoubleFieldParser(Z_FIELD, Z_FIELD));
        fields.put(TYPE, new StringFieldParser(TYPE));
        fields.put(COORDINATES, new DoubleArrayFieldParser(COORDINATES));
    }

    public abstract void assertZValue(boolean ignoreZValue, double zValue);

    public abstract T createPoint(double xValue, double yValue);

    public abstract String fieldError();

    /**
     * Parse a Point with an {@link XContentParser}.
     *
     * @param parser {@link XContentParser} to parse the value from
     * @param ignoreZValue {@link XContentParser} to not throw an error if 3 dimensional data is provided
     * @return new Point parsed from the parser
     */
    public T parsePoint(XContentParser parser, boolean ignoreZValue, Function<String, T> fromString) throws IOException,
        ElasticsearchParseException {
        double x = Double.NaN;
        double y = Double.NaN;
        String geojsonType = null;
        List<Double> coordinates = null;

        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            try (XContentSubParser subParser = new XContentSubParser(parser)) {
                while (subParser.nextToken() != XContentParser.Token.END_OBJECT) {
                    if (subParser.currentToken() == XContentParser.Token.FIELD_NAME) {
                        String field = subParser.currentName();
                        subParser.nextToken();
                        FieldParser<?> fieldParser = fields.get(field);
                        if (fieldParser != null) {
                            switch (fieldParser.name) {
                                case X_FIELD -> x = (Double) fieldParser.parseField(subParser);
                                case Y_FIELD -> y = (Double) fieldParser.parseField(subParser);
                                case Z_FIELD -> assertZValue(ignoreZValue, (Double) fieldParser.parseField(subParser));
                                case TYPE -> geojsonType = (String) fieldParser.parseField(subParser);
                                case COORDINATES -> coordinates = ((DoubleArrayFieldParser) fieldParser).parseField(subParser);
                            }
                        } else {
                            String fieldKeys = collectionToDelimitedString(fields.keySet(), ", ");
                            throw new ElasticsearchParseException("field [{}] not supported - must be one of: {}", field, fieldKeys);
                        }
                    } else {
                        throw new ElasticsearchParseException("token [{}] not allowed", subParser.currentToken());
                    }
                }
            }
            assertOnlyOneFormat(Double.isNaN(x) == false, Double.isNaN(y) == false, coordinates != null, geojsonType != null);
            if (coordinates != null) {
                if (geojsonType == null || geojsonType.toLowerCase(Locale.ROOT).equals("point") == false) {
                    throw new ElasticsearchParseException("[type] for {} can only be 'Point'", mapType);
                }
                if (coordinates.size() < 2) {
                    throw new ElasticsearchParseException("[coordinates] must contain at least two values");
                }
                if (coordinates.size() == 3) {
                    assertZValue(ignoreZValue, coordinates.get(2));
                }
                if (coordinates.size() > 3) {
                    throw new ElasticsearchParseException("[{}] field type does not accept > 3 dimensions", mapType);
                }
                return createPoint(coordinates.get(0), coordinates.get(1));
            }
            return createPoint(x, y);

        } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
            try (XContentSubParser subParser = new XContentSubParser(parser)) {
                int element = 0;
                while (subParser.nextToken() != XContentParser.Token.END_ARRAY) {
                    if (subParser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                        element++;
                        if (element == 1) {
                            x = subParser.doubleValue();
                        } else if (element == 2) {
                            y = subParser.doubleValue();
                        } else if (element == 3) {
                            assertZValue(ignoreZValue, subParser.doubleValue());
                        } else {
                            throw new ElasticsearchParseException("[{}] field type does not accept > 3 dimensions", mapType);
                        }
                    } else {
                        throw new ElasticsearchParseException("numeric value expected");
                    }
                }
            }
            return createPoint(x, y);
        } else if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return fromString.apply(parser.text());
        } else {
            throw new ElasticsearchParseException("{} expected", mapType);
        }
    }

    private static double parseValidDouble(XContentSubParser subParser, String field) throws IOException {
        try {
            return switch (subParser.currentToken()) {
                case VALUE_NUMBER, VALUE_STRING -> subParser.doubleValue(true);
                default -> throw new ElasticsearchParseException("{} must be a number", field);
            };
        } catch (NumberFormatException e) {
            throw new ElasticsearchParseException("[{}] must be a valid double value", e, field);
        }
    }

    private void assertOnlyOneFormat(boolean x, boolean y, boolean coordinates, boolean type) {
        final boolean xy = x && y;
        final boolean geojson = coordinates && type;
        if (xy && geojson) {
            throw new ElasticsearchParseException("fields matching more than one point format");
        } else if ((xy || geojson) == false) {
            if (x) {
                throw new ElasticsearchParseException("Required [{}]", yField);
            } else if (y) {
                throw new ElasticsearchParseException("Required [{}]", xField);
            } else if (coordinates) {
                throw new ElasticsearchParseException("Required [{}]", TYPE);
            } else if (type) {
                throw new ElasticsearchParseException("Required [{}]", COORDINATES);
            } else {
                throw new ElasticsearchParseException(fieldError());
            }
        }
    }
}
