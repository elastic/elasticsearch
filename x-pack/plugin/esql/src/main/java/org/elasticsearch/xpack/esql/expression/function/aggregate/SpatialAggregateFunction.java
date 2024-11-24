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
public abstract class SpatialAggregateFunction extends AggregateFunction {
    protected final FieldExtractPreference preference;

    protected SpatialAggregateFunction(Source source, Expression field, Expression filter, FieldExtractPreference values) {
        super(source, field, filter, emptyList());
        this.preference = values;
    }

    protected SpatialAggregateFunction(StreamInput in, FieldExtractPreference preference) throws IOException {
        super(in);
        // The useDocValues field is only used on data nodes local planning, and therefor never serialized
        this.preference = preference;
    }

    public abstract SpatialAggregateFunction withDocValues();

    @Override
    public boolean checkLicense(XPackLicenseState state) {
        return state.isAllowedByLicense(License.OperationMode.PLATINUM);
    }

    @Override
    public int hashCode() {
        // NB: the hashcode is currently used for key generation so
        // to avoid clashes between aggs with the same arguments, add the class name as variation
        return Objects.hash(getClass(), children(), preference);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            SpatialAggregateFunction other = (SpatialAggregateFunction) obj;
            return Objects.equals(other.field(), field())
                && Objects.equals(other.parameters(), parameters())
                && Objects.equals(other.preference, preference);
        }
        return false;
    }

    public FieldExtractPreference fieldExtractPreference() {
        return preference;
    }
}
