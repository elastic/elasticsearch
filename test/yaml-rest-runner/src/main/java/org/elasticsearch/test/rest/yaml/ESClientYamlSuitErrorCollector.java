/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest.yaml;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;
import org.junit.rules.ErrorCollector;

public class ESClientYamlSuitErrorCollector extends ErrorCollector {

    private final ClientYamlTestExecutionContext restTestExecutionContext;
    private final ClientYamlTestCandidate testCandidate;
    private final Logger logger;

    public ESClientYamlSuitErrorCollector(
        ClientYamlTestExecutionContext restTestExecutionContext,
        ClientYamlTestCandidate testCandidate,
        Logger logger
    ) {
        super();
        this.restTestExecutionContext = restTestExecutionContext;
        this.testCandidate = testCandidate;
        this.logger = logger;
    }

    public void checkSucceeds(ExecutableSection executableSection) {
        super.checkSucceeds(() -> {
            try {
                executableSection.execute(restTestExecutionContext);
            } catch (Throwable t) {
                if (t instanceof AssertionError) {
                    throw t;
                } else {
                    throw new AssertionError(
                        "Error executing section at ["
                            + testCandidate.getSuitePath()
                            + ":"
                            + executableSection.getLocation().lineNumber()
                            + "]: "
                            + t.getMessage(),
                        t
                    );
                }
            }
            return null;
        });
    }

    public void verify() throws AssertionError {
        try {
            super.verify();
        } catch (Throwable e) {
            throw new AssertionError(e.getMessage());
        }
    }
}
