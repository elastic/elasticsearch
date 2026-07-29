/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract base for per-csv-spec-file test classes that run with
 * {@link MappedFieldType.FieldExtractPreference#STORED} forced on every query.
 * <p>
 * The static {@link #csvSpecParameters()} method hides
 * {@link EsqlSpecTestCase#csvSpecParameters()} and applies the stored-loading
 * filter: cases tagged {@code requestStored=SKIP} are dropped, and cases tagged
 * {@code requestStored=IGNORE_ORDER} have {@code ignoreOrder} set to {@code true}.
 * Generated subclasses call {@code csvSpecParameters()} from their {@code @ParametersFactory}
 * factory via the unqualified name, so Java's static-method hiding ensures the
 * filtering version is always used.
 */
public abstract class AbstractEsqlSpecForceStoredLoadingIT extends AbstractEsqlSpecIT {

    protected AbstractEsqlSpecForceStoredLoadingIT(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvTestCase testCase,
        String instructions
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
    }

    @Override
    protected MappedFieldType.FieldExtractPreference fieldExtractPreference() {
        return MappedFieldType.FieldExtractPreference.STORED;
    }

    @Override
    protected boolean ignoreValueOrder() {
        return true;
    }

    @Override
    protected boolean enableRoundingDoubleValuesOnAsserting() {
        return true;
    }

    /**
     * All csv-spec cases (category-ordered, as {@link EsqlSpecTestCase#csvSpecParameters()}), with cases that cannot
     * run under forced stored-field loading filtered out. Hides {@link EsqlSpecTestCase#csvSpecParameters()} so the
     * generated variant class picks up this filtering version through its unqualified {@code csvSpecParameters()} call.
     */
    public static List<Object[]> csvSpecParameters() throws Exception {
        return applyStoredFilter(EsqlSpecTestCase.csvSpecParameters());
    }

    /**
     * Single-file variant of {@link #csvSpecParameters()}: reads the given spec file and applies the same
     * stored-loading filter. Hides {@link EsqlSpecTestCase#readScriptSpec(String)} so that generated per-file
     * classes (which call {@code readScriptSpec("/file.csv-spec")} without qualification) pick up this filtered
     * version through Java's static-method hiding.
     */
    public static List<Object[]> readScriptSpec(String specFile) throws Exception {
        return applyStoredFilter(EsqlSpecTestCase.readScriptSpec(specFile));
    }

    private static List<Object[]> applyStoredFilter(List<Object[]> orig) {
        List<Object[]> specs = new ArrayList<>(orig.size());
        for (Object[] s : orig) {
            CsvTestCase testCase = (CsvTestCase) s[4];
            switch (testCase.requestStored) {
                case SKIP:
                    continue;
                case IGNORE_ORDER:
                    testCase.ignoreOrder = true;
                    break;
                case IGNORE_VALUE_ORDER:
                    break;
                default:
                    throw new AssertionError("unexpected requestStored: " + testCase.requestStored);
            }
            specs.add(s);
        }
        return specs;
    }
}
