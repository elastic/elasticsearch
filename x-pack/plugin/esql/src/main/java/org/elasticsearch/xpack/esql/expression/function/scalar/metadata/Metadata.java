/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.metadata;

import org.elasticsearch.xpack.esql.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.capabilities.UnresolvedException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;

public class Metadata extends UnaryScalarFunction {

    public Metadata(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new Expression.TypeResolution("Unresolved children");
        }
        var resolution = isStringAndExact(field(), sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }
        resolution = isFoldable(field(), sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }
        if (MetadataAttribute.isSupported(metadataFieldName())) {
            return resolution;
        }

        return new Expression.TypeResolution("metadata field [" + field().sourceText() + "] not supported");
    }

    @Override
    public DataType dataType() {
        DataType dataType = MetadataAttribute.dataType(metadataFieldName());
        if (dataType == null) {
            throw new UnresolvedException("dataType", this);
        }
        return dataType;
    }

    public String metadataFieldName() {
        return (String) field().fold();
    }

    @Override
    public boolean foldable() {
        return false;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Metadata(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Metadata::new, field());
    }
}
