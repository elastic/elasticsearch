/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.mapper.MappedFieldType.FieldExtractPreference;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.LicenseAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.Objects;

import static java.util.Collections.emptyList;

/**
 * All spatial aggregate functions extend this class to enable the planning of reading from doc values for higher performance.
 * The AggregateMapper class will generate multiple aggregation functions for each combination, allowing the planner to
 * select the best one.
 */
public abstract class SpatialAggregateFunction extends AggregateFunction implements LicenseAware {
    protected final FieldExtractPreference fieldExtractPreference;

    protected SpatialAggregateFunction(Source source, Expression field, Expression filter, FieldExtractPreference fieldExtractPreference) {
        super(source, field, filter, emptyList());
        this.fieldExtractPreference = fieldExtractPreference;
    }

    protected SpatialAggregateFunction(StreamInput in, FieldExtractPreference fieldExtractPreference) throws IOException {
        super(in);
        // The fieldExtractPreference field is only used on data nodes local planning, and therefore never serialized
        this.fieldExtractPreference = fieldExtractPreference;
    }

    public abstract SpatialAggregateFunction withFieldExtractPreference(FieldExtractPreference preference);

    @Override
    public boolean licenseCheck(XPackLicenseState state) {
        return switch (field().dataType()) {
            case GEO_SHAPE, CARTESIAN_SHAPE -> state.isAllowedByLicense(License.OperationMode.PLATINUM);
            default -> true;
        };
    }

    @Override
    public int hashCode() {
        // NB: the hashcode is currently used for key generation so
        // to avoid clashes between aggs with the same arguments, add the class name as variation
        return Objects.hash(getClass(), children(), fieldExtractPreference);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            SpatialAggregateFunction other = (SpatialAggregateFunction) obj;
            return Objects.equals(other.field(), field())
                && Objects.equals(other.parameters(), parameters())
                && Objects.equals(other.fieldExtractPreference, fieldExtractPreference);
        }
        return false;
    }

    public FieldExtractPreference fieldExtractPreference() {
        return fieldExtractPreference;
    }
}
