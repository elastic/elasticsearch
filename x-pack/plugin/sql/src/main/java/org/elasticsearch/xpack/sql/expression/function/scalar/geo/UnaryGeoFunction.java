/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.geo;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.Locale;
import java.util.Objects;

import static java.lang.String.format;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.sql.expression.SqlTypeResolutions.isGeo;

/**
 * Base class for functions that get a single geo shape or geo point as an argument
 */
public abstract class UnaryGeoFunction extends UnaryScalarFunction {

    protected UnaryGeoFunction(Source source, Expression field) {
        super(source, field);
    }

    @Override
    public Object fold() {
        return operation().apply(field().fold());
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isGeo(field(), operation().toString(), DEFAULT);
    }

    @Override
    protected Processor makeProcessor() {
        return new GeoProcessor(operation());
    }

    protected abstract GeoProcessor.GeoOperation operation();

    @Override
    public ScriptTemplate scriptWithField(FieldAttribute field) {
        //TODO change this to use _source instead of the exact form (aka field.keyword for geo shape fields)
        return new ScriptTemplate(processScript("{sql}.geoDocValue(doc,{})"),
            paramsBuilder().variable(field.exactAttribute().name()).build(),
            dataType());
    }

    @Override
    public String processScript(String template) {
        // basically, transform the script to InternalSqlScriptUtils.[function_name](other_function_or_field_name)
        return super.processScript(
            format(Locale.ROOT, "{sql}.%s(%s)",
                StringUtils.underscoreToLowerCamelCase("ST_" + operation().name()),
                template));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        UnaryGeoFunction other = (UnaryGeoFunction) obj;
        return Objects.equals(other.field(), field());
    }

    @Override
    public int hashCode() {
        return Objects.hash(field());
    }
}
