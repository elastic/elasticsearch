/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.ant;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.taskdefs.condition.Condition;

public class HttpCondition extends HttpTask implements Condition {

    private int expectedResponseCode = 200;

    @Override
    public boolean eval() throws BuildException {
        int responseCode = executeHttpRequest();
        getProject().log("response code=" + responseCode);
        return responseCode == expectedResponseCode;
    }

    public void setExpectedResponseCode(int expectedResponseCode) {
        this.expectedResponseCode = expectedResponseCode;
    }
}
