/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.plan.logical.LocalRelation;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.session.EmptyExecutable;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.Node;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DateUtils;
import org.elasticsearch.xpack.ql.type.DefaultDataTypeRegistry;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.type.TypesTests;
import org.junit.Assert;

import java.io.BufferedReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.TestUtils.of;
import static org.hamcrest.Matchers.instanceOf;

public final class EsqlTestUtils {

    public static final EsqlConfiguration TEST_CFG = new EsqlConfiguration(
        DateUtils.UTC,
        null,
        null,
        Settings.EMPTY,
        EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY)
    );

    private EsqlTestUtils() {}

    public static Literal L(Object value) {
        return of(value);
    }

    public static LogicalPlan emptySource() {
        return new LocalRelation(Source.EMPTY, new EmptyExecutable(emptyList()));
    }

    public static <P extends Node<P>, T extends P> T as(P node, Class<T> type) {
        Assert.assertThat(node, instanceOf(type));
        return type.cast(node);
    }

    public static Map<String, EsField> loadMapping(String name) {
        return TypesTests.loadMapping(DefaultDataTypeRegistry.INSTANCE, name, true);
    }

    public static Tuple<Page, List<String>> loadPage(URL source) throws Exception {

        class CsvColumn {
            String name;
            Type typeConverter;
            List<Object> values;
            Class<?> typeClass = null;
            boolean hasNulls = false;

            CsvColumn(String name, Type typeConverter, List<Object> values) {
                this.name = name;
                this.typeConverter = typeConverter;
                this.values = values;
            }

            void addValue(String value) {
                Object actualValue = typeConverter.convert(value);
                values.add(actualValue);
                if (typeClass == null) {
                    typeClass = actualValue.getClass();
                }
            }

            void addNull() {
                values.add(null);
                this.hasNulls = true;
            }
        }

        CsvColumn[] columns = null;

        try (BufferedReader reader = org.elasticsearch.xpack.ql.TestUtils.reader(source)) {
            String line;
            int lineNumber = 1;

            while ((line = reader.readLine()) != null) {
                line = line.trim();
                // ignore comments
                if (line.isEmpty() == false && line.startsWith("//") == false && line.startsWith("#") == false) {
                    var entries = Strings.delimitedListToStringArray(line, ",");
                    for (int i = 0; i < entries.length; i++) {
                        entries[i] = entries[i].trim();
                    }
                    // the schema row
                    if (columns == null) {
                        columns = new CsvColumn[entries.length];
                        for (int i = 0; i < entries.length; i++) {
                            int split = entries[i].indexOf(":");
                            String name, typeName;

                            if (split < 0) {
                                throw new IllegalArgumentException(
                                    "A type is always expected in the schema definition; found " + entries[i]
                                );
                            } else {
                                name = entries[i].substring(0, split).trim();
                                typeName = entries[i].substring(split + 1).trim();
                                if (typeName.length() == 0) {
                                    throw new IllegalArgumentException(
                                        "A type is always expected in the schema definition; found " + entries[i]
                                    );
                                }
                            }
                            Type type = Type.asType(typeName);
                            if (type == Type.NULL) {
                                throw new IllegalArgumentException("Null type is not allowed in the test data; found " + entries[i]);
                            }
                            columns[i] = new CsvColumn(name, type, new ArrayList<>());
                        }
                    }
                    // data rows
                    else {
                        if (entries.length != columns.length) {
                            throw new IllegalArgumentException(
                                format(
                                    null,
                                    "Error line [{}]: Incorrect number of entries; expected [{}] but found [{}]",
                                    lineNumber,
                                    columns.length,
                                    entries.length
                                )
                            );
                        }
                        for (int i = 0; i < entries.length; i++) {
                            try {
                                if ("".equals(entries[i])) {
                                    columns[i].addNull();
                                } else {
                                    columns[i].addValue(entries[i]);
                                }
                            } catch (Exception e) {
                                throw new IllegalArgumentException(
                                    format(null, "Error line [{}]: Cannot parse entry [{}] with value [{}]", lineNumber, i + 1, entries[i]),
                                    e
                                );
                            }
                        }
                    }
                }
                lineNumber++;
            }
        }
        var blocks = new Block[columns.length];
        var columnNames = new ArrayList<String>(columns.length);
        int i = 0;
        for (CsvColumn c : columns) {
            blocks[i++] = buildBlock(c.values, c.typeClass);
            columnNames.add(c.name);
        }
        return new Tuple<>(new Page(blocks), columnNames);
    }

    static Block buildBlock(List<Object> values, Class<?> type) {
        Block.Builder builder;
        if (type == Integer.class) {
            builder = IntBlock.newBlockBuilder(values.size());
            for (Object v : values) {
                if (v == null) {
                    builder.appendNull();
                } else {
                    ((IntBlock.Builder) builder).appendInt((Integer) v);
                }
            }
        } else if (type == Long.class) {
            builder = LongBlock.newBlockBuilder(values.size());
            for (Object v : values) {
                if (v == null) {
                    builder.appendNull();
                } else {
                    ((LongBlock.Builder) builder).appendLong((Long) v);
                }
            }
        } else if (type == Float.class) {
            // creating a DoubleBlock here, but once a Float one is available this code needs to change
            builder = DoubleBlock.newBlockBuilder(values.size());
            for (Object v : values) {
                if (v == null) {
                    builder.appendNull();
                } else {
                    ((DoubleBlock.Builder) builder).appendDouble((Double) v);
                }
            }
        } else if (type == Double.class) {
            builder = DoubleBlock.newBlockBuilder(values.size());
            for (Object v : values) {
                if (v == null) {
                    builder.appendNull();
                } else {
                    ((DoubleBlock.Builder) builder).appendDouble((Double) v);
                }
            }
        } else {
            // (type == String.class || type == Boolean.class)
            builder = BytesRefBlock.newBlockBuilder(values.size());
            for (Object v : values) {
                if (v == null) {
                    builder.appendNull();
                } else {
                    ((BytesRefBlock.Builder) builder).appendBytesRef(new BytesRef(v.toString()));
                }
            }
        }
        return builder.build();
    }

    public enum Type {
        INTEGER(Integer::parseInt),
        LONG(Long::parseLong),
        DOUBLE(Double::parseDouble),
        KEYWORD(Object::toString),
        NULL(s -> null);

        private final Function<String, Object> converter;

        Type(Function<String, Object> converter) {
            this.converter = converter;
        }

        public static <T extends Enum<T>> T valueOf(Class<T> c, String s) {
            return Enum.valueOf(c, s.trim().toUpperCase(Locale.ROOT));
        }

        public static Type asType(String name) {
            return valueOf(Type.class, name);
        }

        public static Type asType(ElementType elementType) {
            return switch (elementType) {
                case INT -> INTEGER;
                case LONG -> LONG;
                case DOUBLE -> DOUBLE;
                case NULL -> NULL;
                case BYTES_REF -> KEYWORD;
                case UNKNOWN -> {
                    throw new IllegalArgumentException("Unknown block types cannot be handled");
                }
            };
        }

        Object convert(String value) {
            if (value == null) {
                return null;
            }
            return converter.apply(value);
        }
    }
}
