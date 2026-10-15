/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.SequenceSourceTypes;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * Copies mapped block-loader values into the exact typed raw schema expected by a prepared sequence stage. Field fetches
 * preserve native values and multifields; metadata comes from the selected hit. Missing values remain typed nulls,
 * and no source-map reconstruction or timestamp string concatenation is used for correlation.
 */
final class SequenceHitReader {
    private final List<Attribute> attributes;
    private final Configuration configuration;

    SequenceHitReader(List<Attribute> attributes, Configuration configuration) {
        this.attributes = List.copyOf(attributes);
        this.configuration = configuration;
        for (Attribute attribute : attributes) {
            if (attribute instanceof UnmappedFieldsAttribute) {
                throw new IllegalArgumentException("unsupported SEQUENCE unmapped source field [" + attribute.name() + "]");
            }
            if (attribute instanceof FieldAttribute field && SequenceSourceTypes.supports(field.field()) == false) {
                throw new IllegalArgumentException("unsupported SEQUENCE mapped source field [" + attribute.name() + "]");
            }
            if (SequenceSourceTypes.supports(attribute.dataType()) == false) {
                throw new IllegalArgumentException("unsupported SEQUENCE source field type [" + attribute.dataType().typeName() + "]");
            }
        }
    }

    /** Accounts native mapped values while a response or selected hit remains retained between asynchronous callbacks. */
    static long retainedBytes(SearchHit hit) {
        long bytes = hit.ramBytesUsed();
        for (DocumentField field : hit.getDocumentFields().values()) {
            bytes = Math.addExact(bytes, backingBytes(field));
        }
        for (DocumentField field : hit.getMetadataFields().values()) {
            bytes = Math.addExact(bytes, backingBytes(field));
        }
        return bytes;
    }

    /** The generic field estimator charges BytesRef as an unknown object without its copied backing array. */
    static long backingBytes(DocumentField field) {
        long bytes = 0;
        for (Object value : field.getValues()) {
            if (value instanceof BytesRef reference) {
                bytes = Math.addExact(bytes, RamUsageEstimator.sizeOf(reference.bytes));
            }
        }
        return bytes;
    }

    /**
     * Requires the original event timestamp to be scalar before matching or payload replay. Search sort values
     * expose only the minimum of a multivalued date, so they cannot prove the EQL source timestamp contract.
     * Stage projections and MV_EXPAND must not hide or repair an ambiguous original event time.
     */
    static void ensureScalarTimestamp(SearchHit hit) {
        DocumentField field = hit.field("@timestamp");
        if (field == null || field.getValues().size() != 1 || (field.getValues().getFirst() instanceof Long) == false) {
            throw new IllegalArgumentException("SEQUENCE source [@timestamp] must hold a single mapped date or date_nanos value");
        }
    }

    SearchSourceBuilder fields(SearchSourceBuilder source) {
        source.fetchSource(false);
        List<FieldAndFormat> mappedFields = new ArrayList<>();
        // Event time remains mandatory after payload pruning. The mapped loader exposes source cardinality,
        // while the separately returned sort value alone would silently select the earliest array value.
        mappedFields.add(new FieldAndFormat("@timestamp", null));
        for (Attribute attribute : attributes) {
            if (attribute instanceof MetadataAttribute && attribute.name().equals("_version")) {
                source.version(true);
            } else if (attribute.dataType() == DataType.SOURCE) {
                source.fetchSource(true);
            } else if (attribute instanceof MetadataAttribute && attribute.name().equals("_score")) {
                source.trackScores(true);
            } else if (attribute instanceof MetadataAttribute
                && attribute.name().equals("_id") == false
                && attribute.name().equals("_index") == false) {
                    source.fetchField(attribute.name());
                } else if (attribute instanceof FieldAttribute field && attribute.dataType() != DataType.UNSUPPORTED) {
                    mappedFields.add(new FieldAndFormat(fieldName(field), null));
                }
        }
        if (mappedFields.isEmpty() == false) {
            var extensions = new ArrayList<>(source.ext());
            extensions.add(new SequenceDocValuesSearchExt(mappedFields, configuration.pragmas().fieldExtractPreference()));
            source.ext(extensions);
        }
        return source;
    }

    Page read(SearchHit[] hits, BlockFactory factory) {
        Block[] blocks = new Block[attributes.size()];
        boolean success = false;
        try {
            for (int column = 0; column < attributes.size(); column++) {
                Attribute attribute = attributes.get(column);
                ElementType type = elementType(attribute.dataType());
                try (var builder = BlockUtils.wrapperFor(factory, type, hits.length)) {
                    for (SearchHit hit : hits) {
                        if (attribute.dataType() == DataType.NULL || attribute.dataType() == DataType.UNSUPPORTED) {
                            builder.builder().appendNull();
                        } else if (attribute.dataType() == DataType.SOURCE) {
                            builder.accept(hit.getSourceRef() == null ? null : hit.getSourceRef().toBytesRef());
                        } else if (attribute instanceof MetadataAttribute) {
                            builder.accept(metadata(hit, attribute.name()));
                        } else {
                            DocumentField field = hit.field(fieldName(attribute));
                            List<Object> values = field == null ? List.of() : field.getValues();
                            if (values.isEmpty()) {
                                builder.builder().appendNull();
                            } else {
                                if (values.size() > 1) {
                                    builder.builder().beginPositionEntry();
                                }
                                for (Object value : values) {
                                    builder.accept(convert(value, attribute.dataType()));
                                }
                                if (values.size() > 1) {
                                    builder.builder().endPositionEntry();
                                }
                            }
                        }
                    }
                    blocks[column] = builder.builder().build();
                }
            }
            Page page = new Page(hits.length, blocks);
            success = true;
            return page;
        } finally {
            if (success == false) {
                Releasables.close(blocks);
            }
        }
    }

    private Object convert(Object value, DataType type) {
        // Mapped fields arrive in their native compute representation. Preserve encoded IP/version bytes and exact
        // unsigned/date longs rather than interpreting them as display values.
        if (value instanceof BytesRef
            || value instanceof Long && (type == DataType.UNSIGNED_LONG || type == DataType.DATETIME || type == DataType.DATE_NANOS)) {
            return value;
        }
        return switch (type) {
            case KEYWORD, TEXT, FLATTENED -> value == null ? null : value.toString();
            case COUNTER_LONG -> EsqlDataTypeConverter.convert(value, DataType.LONG, configuration);
            case COUNTER_INTEGER -> EsqlDataTypeConverter.convert(value, DataType.INTEGER, configuration);
            case COUNTER_DOUBLE -> EsqlDataTypeConverter.convert(value, DataType.DOUBLE, configuration);
            case UNSIGNED_LONG -> value == null
                ? null
                : NumericUtils.asLongUnsigned(NumericUtils.asUnsignedLong(new BigInteger(value.toString())));
            case BOOLEAN, INTEGER, LONG, DOUBLE, DATETIME, DATE_NANOS, IP, VERSION -> EsqlDataTypeConverter.convert(
                value,
                type,
                configuration
            );
            default -> throw new IllegalArgumentException("unexpected SEQUENCE conversion type [" + type.typeName() + "]");
        };
    }

    private static Object metadata(SearchHit hit, String name) {
        if (name.equals("_id")) {
            return hit.getId();
        }
        if (name.equals("_index")) {
            return hit.getIndex();
        }
        if (name.equals("_version")) {
            return hit.getVersion();
        }
        if (name.equals("_score")) {
            return (double) hit.getScore();
        }
        DocumentField field = hit.field(name);
        return field == null || field.getValues().isEmpty() ? null : field.getValues();
    }

    private static String fieldName(Attribute attribute) {
        return attribute instanceof FieldAttribute field ? field.fieldName().string() : attribute.name();
    }

    private static ElementType elementType(DataType type) {
        return switch (type) {
            case NULL -> ElementType.NULL;
            case BOOLEAN -> ElementType.BOOLEAN;
            case INTEGER, COUNTER_INTEGER -> ElementType.INT;
            case LONG, COUNTER_LONG, UNSIGNED_LONG, DATETIME, DATE_NANOS -> ElementType.LONG;
            case DOUBLE, COUNTER_DOUBLE -> ElementType.DOUBLE;
            case KEYWORD, TEXT, IP, VERSION, SOURCE, FLATTENED, UNSUPPORTED -> ElementType.BYTES_REF;
            default -> throw new IllegalArgumentException("unsupported SEQUENCE source field type [" + type.typeName() + "]");
        };
    }
}
