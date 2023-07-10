/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.qa.verify_version_constants;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.text.ParseException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class VerifyVersionConstantsIT extends ESRestTestCase {

    public void testLuceneVersionConstant() throws IOException, ParseException {
        Response response = client().performRequest(new Request("GET", "/"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        ObjectPath objectPath = ObjectPath.createFromResponse(response);

        String luceneVersionString = objectPath.evaluate("version.lucene_version").toString();
        org.apache.lucene.util.Version luceneVersion = org.apache.lucene.util.Version.parse(luceneVersionString);

        IndexVersion indexVersion;
        Object indexVersionString = objectPath.evaluate("version.index_version");
        if (indexVersionString != null) {
            indexVersion = IndexVersion.fromId(Integer.parseInt(indexVersionString.toString()));
        } else {
            String elasticsearchVersionString = objectPath.evaluate("version.number").toString();
            Version elasticsearchVersion = Version.fromString(elasticsearchVersionString.replace("-SNAPSHOT", ""));
            assertThat(elasticsearchVersion, lessThan(Version.V_8_10_0));
            indexVersion = IndexVersion.fromId(elasticsearchVersion.id);
        }

        assertThat(indexVersion.luceneVersion(), equalTo(luceneVersion));
    }

    @Override
    public boolean preserveClusterUponCompletion() {
        /*
         * We don't perform any writes to the cluster so there won't be anything
         * to clean up. Also, our cleanup code is really only compatible with
         * *write* compatible versions but this runs with *index* compatible
         * versions.
         */
        return true;
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }
}
