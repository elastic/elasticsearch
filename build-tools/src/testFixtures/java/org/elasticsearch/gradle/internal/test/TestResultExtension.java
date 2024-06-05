/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test;

import org.spockframework.runtime.AbstractRunListener;
import org.spockframework.runtime.extension.IGlobalExtension;
import org.spockframework.runtime.model.ErrorInfo;
import org.spockframework.runtime.model.IterationInfo;
import org.spockframework.runtime.model.SpecInfo;

public class TestResultExtension implements IGlobalExtension {

    @Override
    public void visitSpec(SpecInfo spec) {
        spec.addListener(new ErrorListener());
    }

    public static class ErrorListener extends AbstractRunListener {
        ErrorInfo errorInfo;

        @Override
        public void beforeIteration(IterationInfo iteration) {
            errorInfo = null;
        }

        @Override
        public void error(ErrorInfo error) {
            errorInfo = error;
        }
    }
}
