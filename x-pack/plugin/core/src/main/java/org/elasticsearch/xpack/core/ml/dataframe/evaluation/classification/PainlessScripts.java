/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.elasticsearch.script.Script;

import java.text.MessageFormat;
import java.util.Locale;

/**
 * Painless scripts used by classification metrics in this package.
 */
final class PainlessScripts {

    /**
     * Template for the comparison script.
     * It uses "String.valueOf" method in case the mapping types of the two fields are different.
     */
    private static final MessageFormat COMPARISON_SCRIPT_TEMPLATE =
        new MessageFormat("String.valueOf(doc[''{0}''].value).equals(String.valueOf(doc[''{1}''].value))", Locale.ROOT);

    /**
     * Builds script that tests field values equality for the given actual and predicted field names.
     *
     * @param actualField name of the actual field
     * @param predictedField name of the predicted field
     * @return script that tests whether the values of actualField and predictedField are equal
     */
    static Script buildIsEqualScript(String actualField, String predictedField) {
        return new Script(COMPARISON_SCRIPT_TEMPLATE.format(new Object[]{ actualField, predictedField }));
    }
}
