/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.documentation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.security.ChangePasswordRequest;
import org.elasticsearch.client.security.DisableUserRequest;
import org.elasticsearch.client.security.EmptyResponse;
import org.elasticsearch.client.security.EnableUserRequest;
import org.elasticsearch.client.security.GetSslCertificatesResponse;
import org.elasticsearch.client.security.PutRoleMappingRequest;
import org.elasticsearch.client.security.PutRoleMappingResponse;
import org.elasticsearch.client.security.PutUserRequest;
import org.elasticsearch.client.security.PutUserResponse;
import org.elasticsearch.client.security.RefreshPolicy;
import org.elasticsearch.client.security.support.CertificateInfo;
import org.elasticsearch.client.security.support.expressiondsl.RoleMapperExpression;
import org.elasticsearch.client.security.support.expressiondsl.expressions.AnyRoleMapperExpression;
import org.elasticsearch.client.security.support.expressiondsl.fields.FieldRoleMapperExpression;
import org.hamcrest.Matchers;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SecurityDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testPutUser() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            //tag::put-user-execute
            char[] password = new char[]{'p', 'a', 's', 's', 'w', 'o', 'r', 'd'};
            PutUserRequest request =
                new PutUserRequest("example", password, Collections.singletonList("superuser"), null, null, true, null, RefreshPolicy.NONE);
            PutUserResponse response = client.security().putUser(request, RequestOptions.DEFAULT);
            //end::put-user-execute

            //tag::put-user-response
            boolean isCreated = response.isCreated(); // <1>
            //end::put-user-response

            assertTrue(isCreated);
        }

        {
            char[] password = new char[]{'p', 'a', 's', 's', 'w', 'o', 'r', 'd'};
            PutUserRequest request = new PutUserRequest("example2", password, Collections.singletonList("superuser"), null, null, true,
                null, RefreshPolicy.NONE);
            // tag::put-user-execute-listener
            ActionListener<PutUserResponse> listener = new ActionListener<PutUserResponse>() {
                @Override
                public void onResponse(PutUserResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::put-user-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::put-user-execute-async
            client.security().putUserAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::put-user-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testPutRoleMapping() throws Exception {
        final RestHighLevelClient client = highLevelClient();

        {
            // tag::put-role-mapping-execute
            final RoleMapperExpression rules = AnyRoleMapperExpression.builder()
                    .addExpression(FieldRoleMapperExpression.ofUsername("*"))
                    .addExpression(FieldRoleMapperExpression.ofGroups("cn=admins,dc=example,dc=com"))
                    .build();
            final PutRoleMappingRequest request = new PutRoleMappingRequest("mapping-example", true, Collections.singletonList("superuser"),
                    rules, null, RefreshPolicy.NONE);
            final PutRoleMappingResponse response = client.security().putRoleMapping(request, RequestOptions.DEFAULT);
            // end::put-role-mapping-execute
            // tag::put-role-mapping-response
            boolean isCreated = response.isCreated(); // <1>
            // end::put-role-mapping-response
            assertTrue(isCreated);
        }

        {
            final RoleMapperExpression rules = AnyRoleMapperExpression.builder()
                    .addExpression(FieldRoleMapperExpression.ofUsername("*"))
                    .addExpression(FieldRoleMapperExpression.ofGroups("cn=admins,dc=example,dc=com"))
                    .build();
            final PutRoleMappingRequest request = new PutRoleMappingRequest("mapping-example", true, Collections.singletonList("superuser"),
                    rules, null, RefreshPolicy.NONE);
            // tag::put-role-mapping-execute-listener
            ActionListener<PutRoleMappingResponse> listener = new ActionListener<PutRoleMappingResponse>() {
                @Override
                public void onResponse(PutRoleMappingResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::put-role-mapping-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::put-role-mapping-execute-async
            client.security().putRoleMappingAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::put-role-mapping-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testEnableUser() throws Exception {
        RestHighLevelClient client = highLevelClient();
        char[] password = new char[]{'p', 'a', 's', 's', 'w', 'o', 'r', 'd'};
        PutUserRequest putUserRequest = new PutUserRequest("enable_user", password, Collections.singletonList("superuser"), null,
            null, true, null, RefreshPolicy.IMMEDIATE);
        PutUserResponse putUserResponse = client.security().putUser(putUserRequest, RequestOptions.DEFAULT);
        assertTrue(putUserResponse.isCreated());

        {
            //tag::enable-user-execute
            EnableUserRequest request = new EnableUserRequest("enable_user", RefreshPolicy.NONE);
            EmptyResponse response = client.security().enableUser(request, RequestOptions.DEFAULT);
            //end::enable-user-execute

            assertNotNull(response);
        }

        {
            //tag::enable-user-execute-listener
            EnableUserRequest request = new EnableUserRequest("enable_user", RefreshPolicy.NONE);
            ActionListener<EmptyResponse> listener = new ActionListener<EmptyResponse>() {
                @Override
                public void onResponse(EmptyResponse setUserEnabledResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::enable-user-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::enable-user-execute-async
            client.security().enableUserAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::enable-user-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDisableUser() throws Exception {
        RestHighLevelClient client = highLevelClient();
        char[] password = new char[]{'p', 'a', 's', 's', 'w', 'o', 'r', 'd'};
        PutUserRequest putUserRequest = new PutUserRequest("disable_user", password, Collections.singletonList("superuser"), null,
            null, true, null, RefreshPolicy.IMMEDIATE);
        PutUserResponse putUserResponse = client.security().putUser(putUserRequest, RequestOptions.DEFAULT);
        assertTrue(putUserResponse.isCreated());
        {
            //tag::disable-user-execute
            DisableUserRequest request = new DisableUserRequest("disable_user", RefreshPolicy.NONE);
            EmptyResponse response = client.security().disableUser(request, RequestOptions.DEFAULT);
            //end::disable-user-execute

            assertNotNull(response);
        }

        {
            //tag::disable-user-execute-listener
            DisableUserRequest request = new DisableUserRequest("disable_user", RefreshPolicy.NONE);
            ActionListener<EmptyResponse> listener = new ActionListener<EmptyResponse>() {
                @Override
                public void onResponse(EmptyResponse setUserEnabledResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::disable-user-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::disable-user-execute-async
            client.security().disableUserAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::disable-user-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetSslCertificates() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            //tag::get-certificates-execute
            GetSslCertificatesResponse response = client.security().getSslCertificates(RequestOptions.DEFAULT);
            //end::get-certificates-execute

            assertNotNull(response);

            //tag::get-certificates-response
            List<CertificateInfo> certificates = response.getCertificates(); // <1>
            //end::get-certificates-response

            assertThat(certificates.size(), Matchers.equalTo(9));
            final Iterator<CertificateInfo> it = certificates.iterator();
            CertificateInfo c = it.next();
            assertThat(c.getSubjectDn(), Matchers.equalTo("CN=testnode-client-profile"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
            c = it.next();
            assertThat(c.getSubjectDn(), Matchers.equalTo("CN=Elasticsearch Test Node, OU=elasticsearch, O=org"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.crt"));
            assertThat(c.getFormat(), Matchers.equalTo("PEM"));
            c = it.next();
            assertThat(c.getSubjectDn(), Matchers.equalTo("CN=OpenLDAP, OU=Elasticsearch, O=Elastic, L=Mountain View, ST=CA, C=US"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
            c = it.next();
            assertThat(c.getSubjectDn(), Matchers.equalTo("CN=Elasticsearch Test Node, OU=elasticsearch, O=org"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
            c = it.next();
            assertThat(c.getSubjectDn(), Matchers.equalTo("CN=Elasticsearch Test Client, OU=elasticsearch, O=org"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
            c = it.next();
            assertThat(c.getSubjectDn(), Matchers.equalTo("CN=ad-ELASTICSEARCHAD-CA, DC=ad, DC=test, DC=elasticsearch, DC=com"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
            c = it.next();
            assertThat(c.getSubjectDn(), Matchers.equalTo("CN=Elasticsearch Test Node"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
            c = it.next();
            assertThat(c.getSubjectDn(), Matchers.equalTo("CN=samba4"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
            c = it.next();
            assertThat(c.getSubjectDn(), Matchers.equalTo("CN=Elasticsearch Test Node"));
            assertThat(c.getPath(), Matchers.equalTo("testnode.jks"));
            assertThat(c.getFormat(), Matchers.equalTo("jks"));
        }

        {
            // tag::get-certificates-execute-listener
            ActionListener<GetSslCertificatesResponse> listener = new ActionListener<GetSslCertificatesResponse>() {
                @Override
                public void onResponse(GetSslCertificatesResponse getSslCertificatesResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };

            // end::get-certificates-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-certificates-execute-async
            client.security().getSslCertificatesAsync(RequestOptions.DEFAULT, listener); // <1>
            // end::get-certificates-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testChangePassword() throws Exception {
        RestHighLevelClient client = highLevelClient();
        char[] password = new char[]{'p', 'a', 's', 's', 'w', 'o', 'r', 'd'};
        char[] newPassword = new char[]{'n', 'e', 'w', 'p', 'a', 's', 's', 'w', 'o', 'r', 'd'};
        PutUserRequest putUserRequest = new PutUserRequest("change_password_user", password, Collections.singletonList("superuser"),
            null, null, true, null, RefreshPolicy.NONE);
        PutUserResponse putUserResponse = client.security().putUser(putUserRequest, RequestOptions.DEFAULT);
        assertTrue(putUserResponse.isCreated());
        {
            //tag::change-password-execute
            ChangePasswordRequest request = new ChangePasswordRequest("change_password_user", newPassword, RefreshPolicy.NONE);
            EmptyResponse response = client.security().changePassword(request, RequestOptions.DEFAULT);
            //end::change-password-execute

            assertNotNull(response);
        }
        {
            //tag::change-password-execute-listener
            ChangePasswordRequest request = new ChangePasswordRequest("change_password_user", password, RefreshPolicy.NONE);
            ActionListener<EmptyResponse> listener = new ActionListener<EmptyResponse>() {
                @Override
                public void onResponse(EmptyResponse emptyResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::change-password-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            //tag::change-password-execute-async
            client.security().changePasswordAsync(request, RequestOptions.DEFAULT, listener); // <1>
            //end::change-password-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }
}
