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
 * The static {@link #readScriptSpec(String)} method hides
 * {@link EsqlSpecTestCase#readScriptSpec(String)} and applies the stored-loading
 * filter: cases tagged {@code requestStored=SKIP} are dropped, and cases tagged
 * {@code requestStored=IGNORE_ORDER} have {@code ignoreOrder} set to {@code true}.
 * Generated subclasses call this method from their {@code @ParametersFactory}
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
     * Loads the single spec file and filters out cases that cannot run with forced
     * stored-field loading. Hides {@link EsqlSpecTestCase#readScriptSpec(String)}.
     */
    protected static List<Object[]> readScriptSpec(String specFile) throws Exception {
        List<Object[]> orig = EsqlSpecTestCase.readScriptSpec(specFile);
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
