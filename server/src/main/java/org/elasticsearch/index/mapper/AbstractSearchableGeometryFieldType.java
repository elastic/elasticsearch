package org.elasticsearch.index.mapper;

/**
 * a base class for geometry types that support shape query builder
 */
public abstract class AbstractSearchableGeometryFieldType extends MappedFieldType {

    protected AbstractGeometryFieldMapper.QueryProcessor geometryQueryBuilder;

    protected AbstractSearchableGeometryFieldType() {
    }

    protected AbstractSearchableGeometryFieldType(AbstractSearchableGeometryFieldType ref) {
        super(ref);
    }

    public void setGeometryQueryBuilder(AbstractGeometryFieldMapper.QueryProcessor geometryQueryBuilder)  {
        this.geometryQueryBuilder = geometryQueryBuilder;
    }

    public AbstractGeometryFieldMapper.QueryProcessor geometryQueryBuilder() {
        return geometryQueryBuilder;
    }
}

