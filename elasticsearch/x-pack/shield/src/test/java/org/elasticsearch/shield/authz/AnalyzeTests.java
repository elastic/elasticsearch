/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.test.ShieldIntegTestCase;

import java.util.Collections;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.test.ShieldTestsUtils.assertAuthorizationException;
import static org.hamcrest.CoreMatchers.containsString;

public class AnalyzeTests extends ShieldIntegTestCase {
    protected static final String USERS_PASSWD_HASHED = new String(Hasher.BCRYPT.hash(new SecuredString("test123".toCharArray())));

    @Override
    protected String configUsers() {
        return super.configUsers() +
                "analyze_indices:" + USERS_PASSWD_HASHED + "\n" +
                "analyze_cluster:" + USERS_PASSWD_HASHED + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() +
                "analyze_indices:analyze_indices\n" +
                "analyze_cluster:analyze_cluster\n";
    }

    @Override
    protected String configRoles() {
        return super.configRoles()+ "\n" +
                //role that has analyze indices privileges only
                "analyze_indices:\n" +
                "  indices:\n" +
                "    - names: 'test_*'\n" +
                "      privileges: [ 'indices:admin/analyze' ]\n" +
                "analyze_cluster:\n" +
                "  cluster:\n" +
                "    - cluster:admin/analyze\n";
    }

    public void testAnalyzeWithIndices() {
        // this test tries to execute different analyze api variants from a user that has analyze privileges only on a specific index
        // namespace

        createIndex("test_1");
        ensureGreen();

        //ok: user has permissions for analyze on test_*
        SecuredString passwd = new SecuredString("test123".toCharArray());
        client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("analyze_indices", passwd)))
                .admin().indices().prepareAnalyze("this is my text").setIndex("test_1").setAnalyzer("standard").get();

        try {
            //fails: user doesn't have permissions for analyze on index non_authorized
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("analyze_indices", passwd)))
                    .admin().indices().prepareAnalyze("this is my text").setIndex("non_authorized").setAnalyzer("standard").get();
        } catch(ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [indices:admin/analyze] is unauthorized for user [analyze_indices]"));
        }

        try {
            //fails: user doesn't have permissions for cluster level analyze
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("analyze_indices", passwd)))
                    .admin().indices().prepareAnalyze("this is my text").setAnalyzer("standard").get();
        } catch(ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [cluster:admin/analyze] is unauthorized for user [analyze_indices]"));
        }
    }

    public void testAnalyzeWithoutIndices() {
        //this test tries to execute different analyze api variants from a user that has analyze privileges only at cluster level

        SecuredString passwd = new SecuredString("test123".toCharArray());
        try {
            //fails: user doesn't have permissions for analyze on index test_1
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("analyze_cluster", passwd)))
                    .admin().indices().prepareAnalyze("this is my text").setIndex("test_1").setAnalyzer("standard").get();
        } catch(ElasticsearchSecurityException e) {
            assertAuthorizationException(e, containsString("action [indices:admin/analyze] is unauthorized for user [analyze_cluster]"));
        }

        client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("analyze_cluster", passwd)))
                .admin().indices().prepareAnalyze("this is my text").setAnalyzer("standard").get();
    }
}
