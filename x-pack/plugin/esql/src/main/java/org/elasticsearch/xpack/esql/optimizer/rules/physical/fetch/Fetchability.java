/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch;

import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.IndexModeFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.xpack.cluster.routing.allocation.mapper.DataTierFieldMapper;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.CompactMultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;

import java.util.Set;

/**
 * Decides on the coordinator whether the node that owns a document can load an attribute for it later, from the
 * document reference alone.
 * <p>
 * The fetch side runs no planning rule. It loads every attribute through the same per-shard block loader resolution
 * the data drivers use, so an attribute is fetchable only when that resolution alone produces the value the eager
 * path would. Both lists below are allowlists: an attribute or field class that is not listed crosses the exchange
 * as a value, which is always correct.
 */
public final class Fetchability {

    /** Metadata fields that a block loader reads per document. */
    private static final Set<String> FETCHABLE_METADATA = Set.of(
        VersionFieldMapper.NAME,
        MetadataAttribute.INDEX,
        IdFieldMapper.NAME,
        IgnoredFieldMapper.NAME,
        SourceFieldMapper.NAME,
        IndexModeFieldMapper.NAME,
        MetadataAttribute.TSID_FIELD,
        MetadataAttribute.SIZE,
        DataTierFieldMapper.NAME,
        SliceIndexing.FIELD_NAME
    );

    /**
     * Field implementations that need nothing but the shard's mapping to load. Union types qualify because the per
     * index conversion is chosen from the shard's mapping. Fields that may be unmapped do not: a per-node rule chooses
     * between doc values and {@code _source} for them, and the fetch side does not run it.
     */
    private static final Set<Class<? extends EsField>> FETCHABLE_FIELDS = Set.of(
        EsField.class,
        KeywordEsField.class,
        TextEsField.class,
        DateEsField.class,
        MultiTypeEsField.class,
        CompactMultiTypeEsField.class
    );

    private Fetchability() {}

    /**
     * Whether the attribute can be loaded after the cut instead of crossing the exchange. {@code _score} is never
     * fetchable because the query computes it, it is not stored with the document.
     */
    public static boolean isFetchable(Attribute attribute) {
        if (EsQueryExec.isDocAttribute(attribute) || attribute.dataType() == DataType.UNSUPPORTED) {
            return false;
        }
        if (attribute instanceof MetadataAttribute metadata) {
            return FETCHABLE_METADATA.contains(metadata.name());
        }
        // the exact class: subclasses such as the time series metadata attribute are not plain mapped fields
        if (attribute.getClass() == FieldAttribute.class) {
            return FETCHABLE_FIELDS.contains(((FieldAttribute) attribute).field().getClass());
        }
        return false;
    }
}
