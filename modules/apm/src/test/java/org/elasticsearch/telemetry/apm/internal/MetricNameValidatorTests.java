/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal;


import org.elasticsearch.test.ESTestCase;

public class MetricNameValidatorTests extends ESTestCase {

    public void testESPrefixAndDotSeparator(){
        MetricNameValidator nameValidator = new MetricNameValidator();
        nameValidator.validate("es.somemodule.somemetric.count");

        expectThrows(IllegalArgumentException.class, ()-> nameValidator.validate("somemodule.somemetric.count"));
        //verify . is a separator
        expectThrows(IllegalArgumentException.class, ()-> nameValidator.validate("es_somemodule_somemetric_count"));
        expectThrows(IllegalArgumentException.class, ()-> nameValidator.validate("es_somemodule.somemetric.count"));
    }

    public void testNameElementRegex(){
        MetricNameValidator nameValidator = new MetricNameValidator();
        nameValidator.validate("es.somemodulename0.somemetric.count");
        nameValidator.validate("es.some_module_name0.somemetric.count");
        nameValidator.validate("es.s.somemetric.count");

        expectThrows(IllegalArgumentException.class, ()-> nameValidator.validate("es.someModuleName0.somemetric.count"));
        expectThrows(IllegalArgumentException.class, ()-> nameValidator.validate("es.SomeModuleName.somemetric.count"));
        expectThrows(IllegalArgumentException.class, ()-> nameValidator.validate("es.0some_module_name0.somemetric.count"));
        expectThrows(IllegalArgumentException.class, ()-> nameValidator.validate("es.some_#_name0.somemetric.count"));
        expectThrows(IllegalArgumentException.class, ()-> nameValidator.validate("es.some-name0.somemetric.count"));
    }
    public void testLastElementAllowList(){
        MetricNameValidator nameValidator = new MetricNameValidator();
        nameValidator.validate("es.somemodule.somemetric.size");
        nameValidator.validate("es.somemodule.somemetric.total");
        nameValidator.validate("es.somemodule.somemetric.count");
        nameValidator.validate("es.somemodule.somemetric.usage");
        nameValidator.validate("es.somemodule.somemetric.utilization");
        nameValidator.validate("es.somemodule.somemetric.requests");
        expectThrows(IllegalArgumentException.class, ()-> nameValidator.validate("es.somemodule.somemetric.some_other_suffix"));
    }
}
