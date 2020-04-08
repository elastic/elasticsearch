/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.tree.Source;

import java.time.ZoneId;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;

/**
 * Abstract super class for functions like {@link DateTrunc} and {@link DatePart}
 * which require an argument denoting a unit of date/time.
 */
public abstract class BinaryDateTimeDatePartFunction extends BinaryDateTimeFunction {

    public BinaryDateTimeDatePartFunction(Source source, Expression datePart, Expression timestamp, ZoneId zoneId) {
        super(source, datePart, timestamp, zoneId);
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution resolution = isString(left(), sourceText(), Expressions.ParamOrdinal.FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        if (left().foldable()) {
            String datePartValue = (String) left().fold();
            if (datePartValue != null && resolveDateTimeField(datePartValue) == false) {
                List<String> similar = findSimilarDateTimeFields(datePartValue);
                if (similar.isEmpty()) {
                    return new TypeResolution(
                        format(
                            null,
                            "first argument of [{}] must be one of {} or their aliases; found value [{}]",
                            sourceText(),
                            validDateTimeFieldValues(),
                            Expressions.name(left())
                        )
                    );
                } else {
                    return new TypeResolution(
                        format(
                            null,
                            "Unknown value [{}] for first argument of [{}]; did you mean {}?",
                            Expressions.name(left()),
                            sourceText(),
                            similar
                        )
                    );
                }
            }
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    protected abstract boolean resolveDateTimeField(String dateTimeField);

    protected abstract List<String> findSimilarDateTimeFields(String dateTimeField);

    protected abstract List<String> validDateTimeFieldValues();

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), zoneId());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        BinaryDateTimeDatePartFunction that = (BinaryDateTimeDatePartFunction) o;
        return zoneId().equals(that.zoneId());
    }
}
