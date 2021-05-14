/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.condition;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentUtils;
import org.elasticsearch.common.xcontent.ObjectPath;

import java.io.IOException;
import java.time.Clock;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;


public final class CompareCondition extends AbstractCompareCondition {
    public static final String TYPE = "compare";
    private final String path;
    private final Op op;
    private final Object value;

    public CompareCondition(String path, Op op, Object value) {
        this(path, op, value, null);
    }

    CompareCondition(String path, Op op, Object value, Clock clock) {
        super(TYPE, clock);
        this.path = path;
        this.op = op;
        this.value = value;
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

    public static CompareCondition parse(Clock clock, String watchId, XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. expected an object but found [{}] " +
                    "instead", TYPE, watchId, parser.currentToken());
        }
        String path = null;
        Object value = null;
        Op op = null;

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                path = parser.currentName();
            } else if (path == null) {
                throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. expected a field indicating the " +
                        "compared path, but found [{}] instead", TYPE, watchId, token);
            } else if (token == XContentParser.Token.START_OBJECT) {
                token = parser.nextToken();
                if (token != XContentParser.Token.FIELD_NAME) {
                    throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. expected a field indicating the" +
                            " comparison operator, but found [{}] instead", TYPE, watchId, token);
                }
                try {
                    op = Op.resolve(parser.currentName());
                } catch (IllegalArgumentException iae) {
                    throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. unknown comparison operator " +
                            "[{}]", TYPE, watchId, parser.currentName());
                }
                token = parser.nextToken();
                if (op.supportsStructures() == false && token.isValue() == false && token != XContentParser.Token.VALUE_NULL) {
                    throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. compared value for [{}] with " +
                            "operation [{}] must either be a numeric, string, boolean or null value, but found [{}] instead", TYPE,
                            watchId, path, op.name().toLowerCase(Locale.ROOT), token);
                }
                value = XContentUtils.readValue(parser, token);
                token = parser.nextToken();
                if (token != XContentParser.Token.END_OBJECT) {
                    throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. expected end of path object, " +
                            "but found [{}] instead", TYPE, watchId, token);
                }
            } else {
                throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. expected an object for field [{}] " +
                        "but found [{}] instead", TYPE, watchId, path, token);
            }
        }
        return new CompareCondition(path, op, value, clock);
    }

    @Override
    protected Result doExecute(Map<String, Object> model, Map<String, Object> resolvedValues) {
        Object configuredValue = resolveConfiguredValue(resolvedValues, model, value);

        Object resolvedValue = ObjectPath.eval(path, model);
        resolvedValues.put(path, resolvedValue);

        return new Result(resolvedValues, TYPE, op.eval(resolvedValue, configuredValue));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompareCondition condition = (CompareCondition) o;

        if (Objects.equals(path, condition.path) == false) return false;
        if (op != condition.op) return false;
        return Objects.equals(value, condition.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, op, value);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
                .startObject(path)
                .field(op.id(), value)
                .endObject()
                .endObject();
    }

    public enum Op {

        EQ() {
            @Override
            public boolean eval(Object v1, Object v2) {
                Integer compVal = LenientCompare.compare(v1, v2);
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
                Integer compVal = LenientCompare.compare(v1, v2);
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
                Integer compVal = LenientCompare.compare(v1, v2);
                return compVal != null && compVal < 0;
            }
        },
        LTE() {
            @Override
            public boolean eval(Object v1, Object v2) {
                Integer compVal = LenientCompare.compare(v1, v2);
                return compVal != null && compVal <= 0;
            }
        },
        GT() {
            @Override
            public boolean eval(Object v1, Object v2) {
                Integer compVal = LenientCompare.compare(v1, v2);
                return compVal != null && compVal > 0;
            }
        },
        GTE() {
            @Override
            public boolean eval(Object v1, Object v2) {
                Integer compVal = LenientCompare.compare(v1, v2);
                return compVal != null && compVal >= 0;
            }
        };

        public abstract boolean eval(Object v1, Object v2);

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
}
