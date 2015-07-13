/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition.compare;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.watcher.support.xcontent.WatcherXContentUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public class CompareCondition implements Condition {

    public static final String TYPE = "compare";

    private String path;
    private Op op;
    private Object value;

    public CompareCondition(String path, Op op, Object value) {
        this.path = path;
        this.op = op;
        this.value = value;
    }

    @Override
    public final String type() {
        return TYPE;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompareCondition condition = (CompareCondition) o;

        if (!path.equals(condition.path)) return false;
        if (op != condition.op) return false;
        return !(value != null ? !value.equals(condition.value) : condition.value != null);
    }

    @Override
    public int hashCode() {
        int result = path.hashCode();
        result = 31 * result + op.hashCode();
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
                .startObject(path)
                    .field(op.id(), value)
                .endObject()
            .endObject();
    }

    public static CompareCondition parse(String watchId, XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. expected an object but found [{}] instead", TYPE, watchId, parser.currentToken());
        }
        String path = null;
        Object value = null;
        Op op = null;

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                path = parser.currentName();
            } else if (path == null) {
                throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. expected a field indicating the compared path, but found [{}] instead", TYPE, watchId, token);
            } else if (token == XContentParser.Token.START_OBJECT) {
                token = parser.nextToken();
                if (token != XContentParser.Token.FIELD_NAME) {
                    throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. expected a field indicating the comparison operator, but found [{}] instead", TYPE, watchId, token);
                }
                try {
                    op = Op.resolve(parser.currentName());
                } catch (IllegalArgumentException iae) {
                    throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. unknown comparison operator [{}]", TYPE, watchId, parser.currentName());
                }
                token = parser.nextToken();
                if (!op.supportsStructures() && !token.isValue() && token != XContentParser.Token.VALUE_NULL) {
                    throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. compared value for [{}] with operation [{}] must either be a numeric, string, boolean or null value, but found [{}] instead", TYPE, watchId, path, op.name().toLowerCase(Locale.ROOT), token);
                }
                value = WatcherXContentUtils.readValue(parser, token);
                token = parser.nextToken();
                if (token != XContentParser.Token.END_OBJECT) {
                    throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. expected end of path object, but found [{}] instead", TYPE, watchId, token);
                }
            } else {
                throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. expected an object for field [{}] but found [{}] instead", TYPE, watchId, path, token);
            }
        }

        return new CompareCondition(path, op, value);
    }

    public static class Result extends Condition.Result {

        private final @Nullable Map<String, Object> resolveValues;

        Result(Map<String, Object> resolveValues, boolean met) {
            super(TYPE, met);
            this.resolveValues = resolveValues;
        }

        Result(@Nullable Map<String, Object> resolveValues, Exception e) {
            super(TYPE, e);
            this.resolveValues = resolveValues;
        }

        public Map<String, Object> getResolveValues() {
            return resolveValues;
        }

        @Override
        protected XContentBuilder typeXContent(XContentBuilder builder, Params params) throws IOException {
            if (resolveValues == null) {
                return builder;
            }
            return builder.startObject(type)
                    .field(Field.RESOLVED_VALUES.getPreferredName(), resolveValues)
                    .endObject();
        }
    }

    public enum Op {

        EQ() {
            @Override
            public boolean eval(Object v1, Object v2) {
                Integer compVal = compare(v1, v2);
                return compVal != null && compVal == 0;
            }

            @Override
            public boolean supportsStructures() {
                return true;
            }
        },
        NOT_EQ() {
            @Override
            public boolean eval(Object v1, Object v2) {
                Integer compVal = compare(v1, v2);
                return compVal == null || compVal != 0;
            }

            @Override
            public boolean supportsStructures() {
                return true;
            }
        },
        LT() {
            @Override
            public boolean eval(Object v1, Object v2) {
                Integer compVal = compare(v1, v2);
                return compVal != null && compVal < 0;
            }
        },
        LTE() {
            @Override
            public boolean eval(Object v1, Object v2) {
                Integer compVal = compare(v1, v2);
                return compVal != null && compVal <= 0;
            }
        },
        GT() {
            @Override
            public boolean eval(Object v1, Object v2) {
                Integer compVal = compare(v1, v2);
                return compVal != null && compVal > 0;
            }
        },
        GTE() {
            @Override
            public boolean eval(Object v1, Object v2) {
                Integer compVal = compare(v1, v2);
                return compVal != null && compVal >= 0;
            }
        };

        public abstract boolean eval(Object v1, Object v2);

        public boolean supportsStructures() {
            return false;
        }

        // this method performs lenient comparison, potentially between different types. The second argument
        // type (v2) determines the type of comparison (this is because the second argument is configured by the
        // user while the first argument is the dynamic path that is evaluated at runtime. That is, if the user configures
        // a number, it expects a number, therefore the comparison will be based on numeric comparison). If the
        // comparison is numeric, other types (e.g. strings) will converted to numbers if possible, if not, the comparison
        // will fail and `false` will be returned.
        //
        // may return `null` indicating v1 simply doesn't equal v2 (without any order association)
        static Integer compare(Object v1, Object v2) {
            if (Objects.equals(v1, v2)) {
                return 0;
            }
            if (v1 == null || v2 == null) {
                return null;
            }

            // special case for numbers. If v1 is not a number, we'll try to convert it to a number
            if (v2 instanceof Number) {
                if (!(v1 instanceof Number)) {
                    try {
                        v1 = Double.valueOf(String.valueOf(v1));
                    } catch (NumberFormatException nfe) {
                        // could not convert to number
                        return null;
                    }
                }
                return ((Number) v1).doubleValue() > ((Number) v2).doubleValue() ? 1 :
                        ((Number) v1).doubleValue() < ((Number) v2).doubleValue() ? -1 : 0;
            }

            // special case for strings. If v1 is not a string, we'll convert it to a string
            if (v2 instanceof String) {
                v1 = String.valueOf(v1);
                return ((String) v1).compareTo((String) v2);
            }

            // special case for date/times. If v1 is not a dateTime, we'll try to convert it to a datetime
            if (v2 instanceof DateTime) {
                if (v1 instanceof DateTime) {
                    return ((DateTime) v1).compareTo((DateTime) v2);
                }
                if (v1 instanceof String) {
                    try {
                        v1 = WatcherDateTimeUtils.parseDate((String) v1);
                    } catch (Exception e) {
                        return null;
                    }
                } else if (v1 instanceof Number){
                    v1 = new DateTime(((Number) v1).longValue(), DateTimeZone.UTC);
                } else {
                    // cannot convert to date...
                    return null;
                }
                return ((DateTime) v1).compareTo((DateTime) v2);
            }

            if (v1.getClass() != v2.getClass() || Comparable.class.isAssignableFrom(v1.getClass())) {
                return null;
            }

            try {
                return ((Comparable) v1).compareTo(v2);
            } catch (Exception e) {
                return null;
            }
        }

        public String id() {
            return name().toLowerCase(Locale.ROOT);
        }

        public static Op resolve(String id) {
            return Op.valueOf(id.toUpperCase(Locale.ROOT));
        }
    }

    public static class Builder implements Condition.Builder<CompareCondition> {

        private String path;
        private Op op;
        private Object value;

        public Builder(String path, Op op, Object value) {
            this.path = path;
            this.op = op;
            this.value = value;
        }

        public CompareCondition build() {
            return new CompareCondition(path, op, value);
        }
    }

    interface Field extends Condition.Field {
        ParseField RESOLVED_VALUES = new ParseField("resolved_values");
    }
}
