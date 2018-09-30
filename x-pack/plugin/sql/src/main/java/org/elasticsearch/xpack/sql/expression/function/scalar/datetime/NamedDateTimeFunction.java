/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.NamedDateTimeProcessor.NameExtractor;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.UnaryPipe;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.StringUtils;
import org.joda.time.DateTime;

import java.util.Locale;
import java.util.Objects;
import java.util.TimeZone;

import static java.lang.String.format;
import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

/*
 * Base class for "named" date/time functions like month_name and day_name
 */
abstract class NamedDateTimeFunction extends BaseDateTimeFunction {

    NamedDateTimeFunction(Location location, Expression field, TimeZone timeZone) {
        super(location, field, timeZone);
    }

    @Override
    public Object fold() {
        DateTime folded = (DateTime) field().fold();
        if (folded == null) {
            return null;
        }

        return nameExtractor().extract(folded.getMillis(), timeZone().getID());
    }

    @Override
    public ScriptTemplate scriptWithField(FieldAttribute field) {
        return new ScriptTemplate(
                formatTemplate(format(Locale.ROOT, "{sql}.%s(doc[{}].value.millis, {})",
                        StringUtils.underscoreToLowerCamelCase(nameExtractor().name()))),
                paramsBuilder()
                  .variable(field.name())
                  .variable(timeZone().getID()).build(),
                dataType());
    }

    @Override
    protected final Pipe makePipe() {
        return new UnaryPipe(location(), this, Expressions.pipe(field()),
                new NamedDateTimeProcessor(nameExtractor(), timeZone()));
    }

    protected abstract NameExtractor nameExtractor();
    
    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        NamedDateTimeFunction other = (NamedDateTimeFunction) obj;
        return Objects.equals(other.field(), field())
            && Objects.equals(other.timeZone(), timeZone());
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(field(), timeZone());
    }
}