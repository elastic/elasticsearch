/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition.compare.array;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.condition.compare.LenientCompare;
import org.elasticsearch.xpack.common.xcontent.XContentUtils;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class ArrayCompareCondition implements Condition {
    public static final String TYPE = "array_compare";

    private String arrayPath;
    private String path;
    private Op op;
    private Object value;
    private Quantifier quantifier;

    public ArrayCompareCondition(String arrayPath, String path, Op op, Object value, Quantifier quantifier) {
        this.arrayPath = arrayPath;
        this.path = path;
        this.op = op;
        this.value = value;
        this.quantifier = quantifier;
    }

    @Override
    public String type() {
        return TYPE;
    }

    public String getArrayPath() {
        return arrayPath;
    }

    public String getPath() {
        return path;
    }

    public Op getOp() {
        return op;
    }

    public Object getValue() {
        return value;
    }

    public Quantifier getQuantifier() {
        return quantifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ArrayCompareCondition that = (ArrayCompareCondition) o;
        return Objects.equals(getArrayPath(), that.getArrayPath()) &&
                Objects.equals(getPath(), that.getPath()) &&
                Objects.equals(getOp(), that.getOp()) &&
                Objects.equals(getValue(), that.getValue()) &&
                Objects.equals(getQuantifier(), that.getQuantifier());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getArrayPath(), getPath(), getOp(), getValue(), getQuantifier());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return
                builder
                        .startObject()
                            .startObject(arrayPath)
                                .field(Field.PATH.getPreferredName(), path)
                                .startObject(op.id())
                                    .field(Field.VALUE.getPreferredName(), value)
                                    .field(Field.QUANTIFIER.getPreferredName(), quantifier.id())
                                .endObject()
                            .endObject()
                        .endObject();
    }

    public static ArrayCompareCondition parse(String watchId, XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. expected an object but found [{}] " +
                    "instead", TYPE, watchId, parser.currentToken());
        }
        String arrayPath = null;
        String path = null;
        Op op = null;
        Object value = null;
        boolean haveValue = false;
        Quantifier quantifier = null;

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                arrayPath = parser.currentName();
            } else if (arrayPath == null) {
                throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. expected a field indicating the " +
                        "compared path, but found [{}] instead", TYPE, watchId, token);
            } else if (token == XContentParser.Token.START_OBJECT) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        if (ParseFieldMatcher.STRICT.match(parser.currentName(), Field.PATH)) {
                            parser.nextToken();
                            path = parser.text();
                        } else {
                            if (op != null) {
                                throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. encountered " +
                                        "duplicate comparison operator, but already saw [{}].", TYPE, watchId, parser.currentName(), op
                                        .id());
                            }
                            try {
                                op = Op.resolve(parser.currentName());
                            } catch (IllegalArgumentException iae) {
                                throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. unknown comparison " +
                                        "operator [{}]", TYPE, watchId, parser.currentName(), iae);
                            }
                            token = parser.nextToken();
                            if (token == XContentParser.Token.START_OBJECT) {
                                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                    if (token == XContentParser.Token.FIELD_NAME) {
                                        if (ParseFieldMatcher.STRICT.match(parser.currentName(), Field.VALUE)) {
                                            if (haveValue) {
                                                throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. " +
                                                        "encountered duplicate field \"value\", but already saw value [{}].", TYPE,
                                                        watchId, value);
                                            }
                                            token = parser.nextToken();
                                            if (!op.supportsStructures() && !token.isValue() && token != XContentParser.Token.VALUE_NULL) {
                                                throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. " +
                                                        "compared value for [{}] with operation [{}] must either be a numeric, string, " +
                                                        "boolean or null value, but found [{}] instead", TYPE, watchId, path,
                                                        op.name().toLowerCase(Locale.ROOT), token);
                                            }
                                            value = XContentUtils.readValue(parser, token);
                                            haveValue = true;
                                        } else if (ParseFieldMatcher.STRICT.match(parser.currentName(), Field.QUANTIFIER)) {
                                            if (quantifier != null) {
                                                throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. " +
                                                        "encountered duplicate field \"quantifier\", but already saw quantifier [{}].",
                                                        TYPE, watchId, quantifier.id());
                                            }
                                            parser.nextToken();
                                            try {
                                                quantifier = Quantifier.resolve(parser.text());
                                            } catch (IllegalArgumentException iae) {
                                                throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. " +
                                                        "unknown comparison quantifier [{}]", TYPE, watchId, parser.text(), iae);
                                            }
                                        } else {
                                            throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. " +
                                                    "expected a field indicating the comparison value or comparison quantifier, but found" +
                                                    " [{}] instead", TYPE, watchId, parser.currentName());
                                        }
                                    } else {
                                        throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. expected a " +
                                                "field indicating the comparison value or comparison quantifier, but found [{}] instead",
                                                TYPE, watchId, token);
                                    }
                                }
                            } else {
                                throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. expected an object " +
                                        "for field [{}] but found [{}] instead", TYPE, watchId, op.id(), token);
                            }
                        }
                    } else {
                        throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. expected a field indicating" +
                                " the compared path or a comparison operator, but found [{}] instead", TYPE, watchId, token);
                    }
                }
            } else {
                throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. expected an object for field [{}] " +
                        "but found [{}] instead", TYPE, watchId, path, token);
            }
        }

        if (path == null) {
            path = "";
        }
        if (quantifier == null) {
            quantifier = Quantifier.SOME;
        }

        return new ArrayCompareCondition(arrayPath, path, op, value, quantifier);
    }

    public static class Result extends Condition.Result {
        private final @Nullable Map<String, Object> resolvedValues;

        Result(Map<String, Object> resolvedValues, boolean met) {
            super(TYPE, met);
            this.resolvedValues = resolvedValues;
        }

        Result(@Nullable Map<String, Object> resolvedValues, Exception e) {
            super(TYPE, e);
            this.resolvedValues = resolvedValues;
        }

        public Map<String, Object> getResolvedValues() {
            return resolvedValues;
        }

        @Override
        protected XContentBuilder typeXContent(XContentBuilder builder, Params params) throws IOException {
            if (resolvedValues == null) {
                return builder;
            }
            return builder.startObject(type)
                        .field(Field.RESOLVED_VALUES.getPreferredName(), resolvedValues)
                    .endObject();
        }

        public interface Field extends Condition.Field {
            ParseField RESOLVED_VALUES = new ParseField("resolved_values");
        }
    }

    public enum Op {
        EQ() {
            @Override
            public boolean comparison(int x) {
                return x == 0;
            }

            @Override
            public boolean supportsStructures() {
                return true;
            }
        },
        NOT_EQ() {
            @Override
            public boolean comparison(int x) {
                return x != 0;
            }

            @Override
            public boolean supportsStructures() {
                return true;
            }
        },
        GTE() {
            @Override
            public boolean comparison(int x) {
                return x >= 0;
            }
        },
        GT() {
            @Override
            public boolean comparison(int x) {
                return x > 0;
            }
        },
        LTE() {
            @Override
            public boolean comparison(int x) {
                return x <= 0;
            }
        },
        LT() {
            @Override
            public boolean comparison(int x) {
                return x < 0;
            }
        };

        public abstract boolean comparison(int x);

        public boolean supportsStructures() {
            return false;
        }

        public String id() {
            return name().toLowerCase(Locale.ROOT);
        }

        public static Op resolve(String id) {
            return Op.valueOf(id.toUpperCase(Locale.ROOT));
        }
    }

    public enum Quantifier {
        ALL() {
            @Override
            public boolean eval(List<Object> values, Object configuredValue, Op op) {
                for (Object value : values) {
                    Integer compare = LenientCompare.compare(value, configuredValue);
                    boolean comparison = compare != null && op.comparison(compare);
                    if (!comparison) {
                        return false;
                    }
                }
                return true;
            }
        },
        SOME() {
            @Override
            public boolean eval(List<Object> values, Object configuredValue, Op op) {
                for (Object value : values) {
                    Integer compare = LenientCompare.compare(value, configuredValue);
                    boolean comparison = compare != null && op.comparison(compare);
                    if (comparison) {
                        return true;
                    }
                }
                return false;
            }
        };

        public abstract boolean eval(List<Object> values, Object configuredValue, Op op);

        public static Quantifier resolve(String id) {
            return Quantifier.valueOf(id.toUpperCase(Locale.ROOT));
        }

        public String id() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public static Builder builder(String arrayPath, String path, Op op, Object value, Quantifier quantifier) {
        return new Builder(arrayPath, path, op, value, quantifier);
    }

    public static class Builder implements Condition.Builder<ArrayCompareCondition> {
        private String arrayPath;
        private String path;
        private Op op;
        private Object value;
        private Quantifier quantifier;

        private Builder(String arrayPath, String path, Op op, Object value, Quantifier quantifier) {
            this.arrayPath = arrayPath;
            this.path = path;
            this.op = op;
            this.value = value;
            this.quantifier = quantifier;
        }

        public ArrayCompareCondition build() {
            return new ArrayCompareCondition(arrayPath, path, op, value, quantifier);
        }
    }

    interface Field {
        ParseField PATH = new ParseField("path");
        ParseField VALUE = new ParseField("value");
        ParseField QUANTIFIER = new ParseField("quantifier");
    }
}
