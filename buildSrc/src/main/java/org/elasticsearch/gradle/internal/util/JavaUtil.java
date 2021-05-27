/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.util;

import org.elasticsearch.gradle.internal.info.BuildParams;
import org.elasticsearch.gradle.internal.info.JavaHome;
import org.gradle.api.GradleException;

import java.util.List;
import java.util.Optional;

public class JavaUtil {

    /** A convenience method for getting java home for a version of java and requiring that version for the given task to execute */
    public static String getJavaHome(final int version) {
        List<JavaHome> javaHomes = BuildParams.getJavaVersions();
        Optional<JavaHome> java = javaHomes.stream().filter(j -> j.getVersion() == version).findFirst();
        return java.orElseThrow(() -> new GradleException("JAVA" + version + "_HOME required")).getJavaHome().get().getAbsolutePath();
    }
}
