/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.type.SqlDataTypeConverter;

import java.time.ZoneId;
import java.util.Objects;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;

public class Cast extends UnaryScalarFunction {

    private final DataType dataType;
    private final ZoneId zoneId;

    public Cast(Source source, Expression field, DataType dataType, ZoneId zoneId) {
        super(source, field);
        this.dataType = dataType;
        this.zoneId = zoneId;
    }

    @Override
    protected NodeInfo<Cast> info() {
        return NodeInfo.create(this, Cast::new, field(), dataType, zoneId);
    }

    @Override
    protected UnaryScalarFunction replaceChild(Expression newChild) {
        return new Cast(source(), newChild, dataType, zoneId);
    }

    public DataType from() {
        return field().dataType();
    }

    public DataType to() {
        return dataType;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    public ZoneId zoneId() { return zoneId; }

    @Override
    public boolean foldable() {
        return field().foldable();
    }

    @Override
    public Object fold() {
        return SqlDataTypeConverter.convert(field().fold(), dataType, zoneId);
    }

    @Override
    public Nullability nullable() {
        return Expressions.isNull(field()) ? Nullability.TRUE : Nullability.UNKNOWN;
    }

    @Override
    protected TypeResolution resolveType() {
        return SqlDataTypeConverter.canConvert(from(), to()) ?
                TypeResolution.TYPE_RESOLVED :
                    new TypeResolution("Cannot cast [" + from() + "] to [" + to()+ "]");
    }

    @Override
    protected Processor makeProcessor() {
        return new CastProcessor(SqlDataTypeConverter.converterFor(from(), to()), zoneId);
    }

    @Override
    public ScriptTemplate asScript() {
        ScriptTemplate fieldAsScript = asScript(field());

        return new ScriptTemplate(
            formatTemplate(format("{sql}.", "cast({},{},{})", fieldAsScript.template())),
            paramsBuilder()
                .script(fieldAsScript.params())
                .variable(dataType.name())
                .variable(zoneId.getId()).build(),
            dataType()
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dataType, zoneId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        Cast other = (Cast) obj;
        return Objects.equals(dataType, other.dataType())
            && Objects.equals(field(), other.field());
    }
}
