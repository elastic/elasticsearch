/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.condition;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentUtils;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public final class ArrayCompareCondition extends AbstractCompareCondition {

    public static final String TYPE = "array_compare";
    private final String arrayPath;
    private final String path;
    private final Op op;
    private final Object value;
    private final Quantifier quantifier;

    ArrayCompareCondition(String arrayPath, String path, Op op, Object value, Quantifier quantifier, Clock clock) {
        super(TYPE, clock);
        this.arrayPath = arrayPath;
        this.path = path;
        this.op = op;
        this.value = value;
        this.quantifier = quantifier;
    }

    public String getArrayPath() {
        return arrayPath;
    }

    public String getPath() {
        return path;
    }

    public ArrayCompareCondition.Op getOp() {
        return op;
    }

    public Object getValue() {
        return value;
    }

    public ArrayCompareCondition.Quantifier getQuantifier() {
        return quantifier;
    }

    public static ArrayCompareCondition parse(Clock clock, String watchId, XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException(
                "could not parse [{}] condition for watch [{}]. expected an object but found [{}] " + "instead",
                TYPE,
                watchId,
                parser.currentToken()
            );
        }
        String arrayPath = null;
        String path = null;
        Op op = null;
        Object value = null;
        Quantifier quantifier = null;

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                arrayPath = parser.currentName();
            } else if (arrayPath == null) {
                throw new ElasticsearchParseException(
                    "could not parse [{}] condition for watch [{}]. expected a field indicating the "
                        + "compared path, but found [{}] instead",
                    TYPE,
                    watchId,
                    token
                );
            } else if (token == XContentParser.Token.START_OBJECT) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        if (parser.currentName().equals("path")) {
                            parser.nextToken();
                            path = parser.text();
                        } else {
                            try {
                                op = Op.resolve(parser.currentName());
                            } catch (IllegalArgumentException iae) {
                                throw new ElasticsearchParseException(
                                    "could not parse [{}] condition for watch [{}]. unknown comparison " + "operator [{}]",
                                    TYPE,
                                    watchId,
                                    parser.currentName(),
                                    iae
                                );
                            }
                            token = parser.nextToken();
                            if (token == XContentParser.Token.START_OBJECT) {
                                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                    if (token == XContentParser.Token.FIELD_NAME) {
                                        if (parser.currentName().equals("value")) {
                                            token = parser.nextToken();
                                            if (op.supportsStructures() == false
                                                && token.isValue() == false
                                                && token != XContentParser.Token.VALUE_NULL) {
                                                throw new ElasticsearchParseException(
                                                    "could not parse [{}] condition for watch [{}]. "
                                                        + "compared value for [{}] with operation [{}] must either be a numeric, string, "
                                                        + "boolean or null value, but found [{}] instead",
                                                    TYPE,
                                                    watchId,
                                                    path,
                                                    op.name().toLowerCase(Locale.ROOT),
                                                    token
                                                );
                                            }
                                            value = XContentUtils.readValue(parser, token);
                                        } else if (parser.currentName().equals("quantifier")) {
                                            parser.nextToken();
                                            try {
                                                quantifier = Quantifier.resolve(parser.text());
                                            } catch (IllegalArgumentException iae) {
                                                throw new ElasticsearchParseException(
                                                    "could not parse [{}] condition for watch [{}]. "
                                                        + "unknown comparison quantifier [{}]",
                                                    TYPE,
                                                    watchId,
                                                    parser.text(),
                                                    iae
                                                );
                                            }
                                        } else {
                                            throw new ElasticsearchParseException(
                                                "could not parse [{}] condition for watch [{}]. "
                                                    + "expected a field indicating the comparison value or comparison quantifier, but found"
                                                    + " [{}] instead",
                                                TYPE,
                                                watchId,
                                                parser.currentName()
                                            );
                                        }
                                    } else {
                                        throw new ElasticsearchParseException(
                                            "could not parse [{}] condition for watch [{}]. expected a "
                                                + "field indicating the comparison value or comparison quantifier, but found [{}] instead",
                                            TYPE,
                                            watchId,
                                            token
                                        );
                                    }
                                }
                            } else {
                                throw new ElasticsearchParseException(
                                    "could not parse [{}] condition for watch [{}]. expected an object "
                                        + "for field [{}] but found [{}] instead",
                                    TYPE,
                                    watchId,
                                    op.id(),
                                    token
                                );
                            }
                        }
                    } else {
                        throw new ElasticsearchParseException(
                            "could not parse [{}] condition for watch [{}]. expected a field indicating"
                                + " the compared path or a comparison operator, but found [{}] instead",
                            TYPE,
                            watchId,
                            token
                        );
                    }
                }
            } else {
                throw new ElasticsearchParseException(
                    "could not parse [{}] condition for watch [{}]. expected an object for field [{}] " + "but found [{}] instead",
                    TYPE,
                    watchId,
                    path,
                    token
                );
            }
        }

        if (path == null) {
            path = "";
        }
        if (quantifier == null) {
            quantifier = Quantifier.SOME;
        }

        return new ArrayCompareCondition(arrayPath, path, op, value, quantifier, clock);
    }

    public Result doExecute(Map<String, Object> model, Map<String, Object> resolvedValues) {
        Object configuredValue = resolveConfiguredValue(resolvedValues, model, value);

        Object object = ObjectPath.eval(arrayPath, model);
        if (object != null && (object instanceof List) == false) {
            throw new IllegalStateException("array path " + arrayPath + " did not evaluate to array, was " + object);
        }

        @SuppressWarnings("unchecked")
        List<Object> resolvedArray = object != null ? (List<Object>) object : Collections.emptyList();

        List<Object> resolvedValue = new ArrayList<>(resolvedArray.size());
        for (int i = 0; i < resolvedArray.size(); i++) {
            resolvedValue.add(ObjectPath.eval(path, resolvedArray.get(i)));
        }
        resolvedValues.put(arrayPath, resolvedArray);

        return new Result(resolvedValues, TYPE, quantifier.eval(resolvedValue, configuredValue, op));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ArrayCompareCondition that = (ArrayCompareCondition) o;
        return Objects.equals(getArrayPath(), that.getArrayPath())
            && Objects.equals(getPath(), that.getPath())
            && Objects.equals(getOp(), that.getOp())
            && Objects.equals(getValue(), that.getValue())
            && Objects.equals(getQuantifier(), that.getQuantifier());
    }

    @Override
    public int hashCode() {
        return Objects.hash(arrayPath, path, op, value, quantifier);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .startObject(arrayPath)
            .field("path", path)
            .startObject(op.id())
            .field("value", value)
            .field("quantifier", quantifier.id())
            .endObject()
            .endObject()
            .endObject();
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
                    if (comparison == false) {
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
}
