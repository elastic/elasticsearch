/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.search.aggregations.metrics.PercentilesConfig;
import org.elasticsearch.search.aggregations.metrics.PercentilesMethod;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Foldables;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.type.SqlDataTypeConverter;

public interface HasPercentileConfig {

    Expression method();

    Expression methodParameter();

    default PercentilesConfig percentileConfig() {
        return asPercentileConfig(method(), methodParameter());
    }

    private static PercentilesConfig asPercentileConfig(Expression method, Expression methodParameter) {
        if (method == null) {
            return new PercentilesConfig.TDigest();
        }
        String methodName = foldOptionalNullable(method, DataTypes.KEYWORD);
        PercentilesMethod percentilesMethod = null;
        for (PercentilesMethod m : PercentilesMethod.values()) {
            if (m.getParseField().getPreferredName().equals(methodName)) {
                percentilesMethod = m;
                break;
            }
        }
        if (percentilesMethod == null) {
            throw new IllegalStateException("Not handled PercentilesMethod [" + methodName + "], type resolution needs fix");
        }
        switch (percentilesMethod) {
            case TDIGEST:
                Double compression = foldOptionalNullable(methodParameter, DataTypes.DOUBLE);
                return compression == null ? new PercentilesConfig.TDigest() : new PercentilesConfig.TDigest(compression);
            case HDR:
                Integer numOfDigits = foldOptionalNullable(methodParameter, DataTypes.INTEGER);
                return numOfDigits == null ? new PercentilesConfig.Hdr() : new PercentilesConfig.Hdr(numOfDigits);
            default:
                throw new IllegalStateException("Not handled PercentilesMethod [" + percentilesMethod + "], type resolution needs fix");
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T foldOptionalNullable(Expression e, DataType dataType) {
        if (e == null) {
            return null;
        }
        return (T) SqlDataTypeConverter.convert(Foldables.valueOf(e), dataType);
    }

}
