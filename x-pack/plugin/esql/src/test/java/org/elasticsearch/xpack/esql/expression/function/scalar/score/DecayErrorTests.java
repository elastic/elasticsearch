/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.score;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ErrorsForCasesWithoutExamplesTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matcher;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class DecayErrorTests extends ErrorsForCasesWithoutExamplesTestCase {

    @Override
    protected List<TestCaseSupplier> cases() {
        return paramsToSuppliers(DecayTests.parameters());
    }

    @Override
    protected Stream<List<DataType>> testCandidates(List<TestCaseSupplier> cases, Set<List<DataType>> valid) {
        Set<List<DataType>> validWithAcceptedTypes = new HashSet<>(valid);
        // The valid origin and scale types depend on the value type, so expand beyond the examples in DecayTests.
        for (DataType valueDataType : DataType.types()) {
            if (validValueType(valueDataType) == false) {
                continue;
            }
            for (DataType originDataType : DataType.types()) {
                if (validOriginType(valueDataType, originDataType) == false) {
                    continue;
                }
                for (DataType scaleDataType : DataType.types()) {
                    if (validScaleType(valueDataType, scaleDataType)) {
                        validWithAcceptedTypes.add(List.of(valueDataType, originDataType, scaleDataType, DataType.SOURCE));
                    }
                }
            }
        }
        return super.testCandidates(cases, validWithAcceptedTypes);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        // DecayTests marks options as SOURCE, but Decay's resolver expects an actual MapExpression.
        Expression options = args.get(3).dataType() == DataType.SOURCE ? new MapExpression(source, List.of()) : args.get(3);
        return new Decay(source, args.get(0), args.get(1), args.get(2), options);
    }

    @Override
    protected Matcher<String> expectedTypeErrorMatcher(List<Set<DataType>> validPerPosition, List<DataType> signature) {
        return equalTo(errorMessageStringForDecay(signature));
    }

    private static String errorMessageStringForDecay(List<DataType> signature) {
        DataType valueDataType = signature.get(0);
        DataType originDataType = signature.get(1);
        DataType scaleDataType = signature.get(2);
        DataType optionsDataType = signature.get(3);

        if (valueDataType == DataType.NULL) {
            return nullTypeError(signature, 0);
        }
        if (validValueType(valueDataType) == false) {
            return typeError(signature, 0, "numeric, date or spatial point");
        }

        // Options are resolved before origin/scale compatibility, so mirror that order here.
        if (optionsDataType == DataType.NULL) {
            return nullTypeError(signature, 3);
        }
        if (optionsDataType != DataType.SOURCE) {
            return "fourth argument of [" + sourceForSignature(signature) + "] must be a map expression, received []";
        }

        if (originDataType == DataType.NULL) {
            return nullTypeError(signature, 1);
        }
        if (validOriginType(valueDataType, originDataType) == false) {
            return typeError(signature, 1, originTypeDescription(valueDataType));
        }

        if (scaleDataType == DataType.NULL) {
            return nullTypeError(signature, 2);
        }
        if (validScaleType(valueDataType, scaleDataType) == false) {
            return typeError(signature, 2, scaleTypeDescription(valueDataType));
        }

        throw new IllegalStateException(
            "Can't generate error message for these types, you probably need a custom signature = " + signature
        );
    }

    private static boolean validValueType(DataType dataType) {
        return dataType.isNumeric() || dataType.isDate() || DataType.isSpatialPoint(dataType);
    }

    private static boolean validOriginType(DataType valueDataType, DataType originDataType) {
        if (DataType.isSpatialPoint(valueDataType)) {
            return DataType.isSpatialPoint(originDataType);
        }
        if (DataType.isMillisOrNanos(valueDataType)) {
            return DataType.isMillisOrNanos(originDataType);
        }
        return originDataType.isNumeric();
    }

    private static boolean validScaleType(DataType valueDataType, DataType scaleDataType) {
        if (DataType.isSpatialPoint(valueDataType)) {
            return DataType.isGeoPoint(valueDataType) ? DataType.isString(scaleDataType) : scaleDataType.isNumeric();
        }
        if (DataType.isMillisOrNanos(valueDataType)) {
            return DataType.isTimeDuration(scaleDataType);
        }
        return scaleDataType.isNumeric();
    }

    private static String originTypeDescription(DataType valueDataType) {
        if (DataType.isSpatialPoint(valueDataType)) {
            return "spatial point";
        }
        if (DataType.isMillisOrNanos(valueDataType)) {
            return "datetime or date_nanos";
        }
        return "numeric";
    }

    private static String scaleTypeDescription(DataType valueDataType) {
        if (DataType.isSpatialPoint(valueDataType)) {
            return DataType.isGeoPoint(valueDataType) ? "keyword or text" : "numeric";
        }
        if (DataType.isMillisOrNanos(valueDataType)) {
            return "time_duration";
        }
        return "numeric";
    }

    private static String nullTypeError(List<DataType> signature, int position) {
        return ordinal(position) + "argument of [" + sourceForSignature(signature) + "] cannot be null, received []";
    }

    private static String typeError(List<DataType> signature, int position, String expectedTypeString) {
        return ordinal(position)
            + "argument of ["
            + sourceForSignature(signature)
            + "] must be ["
            + expectedTypeString
            + "], found value [] type ["
            + signature.get(position).typeName()
            + "]";
    }

    private static String ordinal(int position) {
        return TypeResolutions.ParamOrdinal.fromIndex(position).name().toLowerCase(Locale.ROOT) + " ";
    }
}
