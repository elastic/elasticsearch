/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.evaluation.classification;

import org.elasticsearch.script.Script;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class PainlessScriptsTests extends ESTestCase {

    public void testBuildIsEqualScript() {
        Script script = PainlessScripts.buildIsEqualScript("act", "pred");
        assertThat(script.getIdOrCode(), equalTo("String.valueOf(doc['act'].value).equals(String.valueOf(doc['pred'].value))"));
    }
}
