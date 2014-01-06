/*
 * Licensed to Elasticsearch under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elasticsearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.test.rest.junit;

import com.google.common.base.Joiner;
import org.elasticsearch.test.rest.section.RestTestSuite;
import org.elasticsearch.test.rest.section.TestSection;
import org.junit.runner.Description;

import java.util.Map;

/**
 * Helper that knows how to assign proper junit {@link Description}s to each of the node in the tests tree
 */
public final class DescriptionHelper {

    private DescriptionHelper() {

    }

    /*
    The following generated ids need to be unique throughout a tests run.
    Ids are also shown by IDEs (with junit 4.11 unique ids can be different from what gets shown, not yet in 4.10).
    Some tricks are applied to control what gets shown in IDEs in order to keep the ids unique and nice to see at the same time.
     */

    static Description createRootDescription(String name) {
        return Description.createSuiteDescription(name);
    }

    static Description createApiDescription(String api) {
        return Description.createSuiteDescription(api);
    }

    static Description createTestSuiteDescription(RestTestSuite restTestSuite) {
        //e.g. "indices_open (10_basic)", which leads to 10_basic being returned by Description#getDisplayName
        String name = restTestSuite.getApi() + " (" + restTestSuite.getName() + ")";
        return Description.createSuiteDescription(name);
    }

    static Description createTestSectionWithRepetitionsDescription(RestTestSuite restTestSuite, TestSection testSection) {
        //e.g. "indices_open/10_basic (Basic test for index open/close)", which leads to
        //"Basic test for index open/close" being returned by Description#getDisplayName
        String name = restTestSuite.getDescription() + " (" + testSection.getName() + ")";
        return Description.createSuiteDescription(name);
    }

    static Description createTestSectionIterationDescription(RestTestSuite restTestSuite, TestSection testSection, Map<String, Object> args) {
        //e.g. "Basic test for index open/close {#0} (indices_open/10_basic)" some IDEs might strip out the part between parentheses
        String name = testSection.getName() + formatMethodArgs(args) + " ("  + restTestSuite.getDescription() + ")";
        return Description.createSuiteDescription(name);
    }

    private static String formatMethodArgs(Map<String, Object> args) {
        if (args == null || args.isEmpty()) return "";

        StringBuilder b = new StringBuilder(" {");
        Joiner.on(" ").withKeyValueSeparator("").appendTo(b, args);
        b.append("}");

        return b.toString();
    }
}
