package org.elasticsearch.index.mapper;

import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.ConstantIndexFieldData;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

@Deprecated
public final class TypeFieldType extends ConstantFieldType {

    public static final String NAME = "_type";

    public static final TypeFieldType INSTANCE = new TypeFieldType();

    private TypeFieldType() {
        freeze();
    }

    @Override
    public MappedFieldType clone() {
        return this;
    }

    @Override
    public String typeName() {
        return NAME;
    }

    @Override
    public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
        return new ConstantIndexFieldData.Builder(s -> MapperService.SINGLE_MAPPING_NAME);
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return CoreValuesSourceType.BYTES;
    }

    @Override
    protected boolean matches(String pattern, QueryShardContext context) {
        return pattern.equals(MapperService.SINGLE_MAPPING_NAME);
    }
}
