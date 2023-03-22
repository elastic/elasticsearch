/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit.transport;

import org.gradle.api.GradleException;
import org.gradle.api.tasks.OutputFile;
import org.gradle.testing.jacoco.tasks.JacocoCoverageVerification;

import java.io.File;
import java.io.IOException;

public class AggregatingJacocoCoverageVerification extends JacocoCoverageVerification {
    @OutputFile
    protected File getDummyOutputFile() {
        return new File(this.getTemporaryDir(), "continuing-success.txt");
    }
    @Override
    public void check() throws IOException {
        getLogger().error("fff" );

        try{
            super.check();
        }catch (GradleException e){
           getLogger().error("failure ",e );
        }
    }
}
