/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.action.admin.indices.AliasesNotFoundException;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.test.SecurityTestsUtils.assertThrowsAuthorizationException;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;

public class IndexAliasesTests extends SecurityIntegTestCase {

    @Override
    protected String configUsers() {
        final String usersPasswdHashed = new String(getFastStoredHashAlgoForTests().hash(new SecureString
            ("test123".toCharArray())));
        return super.configUsers() +
            "create_only:" + usersPasswdHashed + "\n" +
            "create_test_aliases_test:" + usersPasswdHashed + "\n" +
            "create_test_aliases_alias:" + usersPasswdHashed + "\n" +
            "create_test_aliases_test_alias:" + usersPasswdHashed + "\n" +
            "aliases_only:" + usersPasswdHashed + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() +
                "create_only:create_only\n" +
                "create_test_aliases_test:create_test_aliases_test\n" +
                "create_test_aliases_alias:create_test_aliases_alias\n" +
                "create_test_aliases_test_alias:create_test_aliases_test_alias\n" +
                "aliases_only:aliases_only\n";
    }

    @Override
    protected String configRoles() {
        return super.configRoles() + "\n" +
                //role that has create index only privileges
                "create_only:\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [ create_index ]\n" +
                //role that has create index and manage_aliases on test_*, not enough to manage_aliases aliases outside of test_* namespace
                "create_test_aliases_test:\n" +
                "  indices:\n" +
                "    - names: 'test_*'\n" +
                "      privileges: [ create_index, 'indices:admin/aliases*' ]\n" +
                //role that has create index on test_* and manage_aliases on alias_*, can't create aliases pointing to test_* though
                "create_test_aliases_alias:\n" +
                "  indices:\n" +
                "    - names: 'test_*'\n" +
                "      privileges: [ create_index ]\n" +
                "    - names: 'alias_*'\n" +
                "      privileges: [ 'indices:admin/aliases*' ]\n" +
                //role that has create index on test_* and manage_aliases on both alias_* and test_*
                "create_test_aliases_test_alias:\n" +
                "  indices:\n" +
                "    - names: 'test_*'\n" +
                "      privileges: [ create_index ]\n" +
                "    - names: [ 'alias_*', 'test_*' ]\n" +
                "      privileges: [ 'indices:admin/aliases*' ]\n" +
                //role that has manage_aliases only on both test_* and alias_*
                "aliases_only:\n" +
                "  indices:\n" +
                "    - names: [ 'alias_*', 'test_*']\n" +
                "      privileges: [ 'indices:admin/aliases*' ]\n";
    }

    @Before
    public void createBogusIndex() {
        if (randomBoolean()) {
            //randomly create an index with two aliases from user admin, to make sure it doesn't affect any of the test results
            assertAcked(client().admin().indices().prepareCreate("index1").addAlias(new Alias("alias1")).addAlias(new Alias("alias2")));
        }
    }

    public void testCreateIndexThenAliasesCreateOnlyPermission() {
        //user has create permission only: allows to create indices, manage_aliases is required to add/remove aliases
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_only",
                new SecureString("test123".toCharArray())));
        assertAcked(client().filterWithHeader(headers).admin().indices().prepareCreate("test_1").get());

        assertThrowsAuthorizationException(
                client().filterWithHeader(headers).admin().indices().prepareAliases().addAlias("test_1", "test_alias")::get,
                IndicesAliasesAction.NAME, "create_only");

        assertThrowsAuthorizationException(client().filterWithHeader(headers).admin().indices().prepareAliases()
                .addAlias("test_*", "test_alias")::get, IndicesAliasesAction.NAME, "create_only");
    }

    public void testCreateIndexAndAliasesCreateOnlyPermission() {
        //user has create permission only: allows to create indices, manage_aliases is required to add aliases although they are part of
        // the same create index request
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_only",
                new SecureString("test123".toCharArray())));

        assertThrowsAuthorizationException(
                client().filterWithHeader(headers).admin().indices().prepareCreate("test_1").addAlias(new Alias("test_2"))::get,
                IndicesAliasesAction.NAME, "create_only");
    }

    public void testDeleteAliasesCreateOnlyPermission() {
        //user has create permission only: allows to create indices, manage_aliases is required to add/remove aliases
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_only",
                new SecureString("test123".toCharArray())));

        assertThrowsAuthorizationException(
                client().filterWithHeader(headers).admin().indices().prepareAliases().removeAlias("test_1", "alias_1")::get,
                IndicesAliasesAction.NAME, "create_only");

        assertThrowsAuthorizationException(client().filterWithHeader(headers).admin().indices().prepareAliases()
                .removeAlias("test_1", "alias_*")::get, IndicesAliasesAction.NAME, "create_only");

        assertThrowsAuthorizationException(client().filterWithHeader(headers).admin().indices().prepareAliases()
                .removeAlias("test_1", "_all")::get, IndicesAliasesAction.NAME, "create_only");
    }

    public void testGetAliasesCreateOnlyPermissionStrict() {
        //user has create permission only: allows to create indices, manage_aliases is required to retrieve aliases though
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_only",
                new SecureString("test123".toCharArray())));

        assertThrowsAuthorizationException(client().filterWithHeader(headers).admin().indices().prepareGetAliases("test_1")
                .setIndices("test_1").setIndicesOptions(IndicesOptions.strictExpand())::get, GetAliasesAction.NAME, "create_only");

        assertThrowsAuthorizationException(client().filterWithHeader(headers)
                .admin().indices().prepareGetAliases("_all")
                .setIndices("test_1").setIndicesOptions(IndicesOptions.strictExpand())::get, GetAliasesAction.NAME, "create_only");

        assertThrowsAuthorizationException(client().filterWithHeader(headers).admin().indices()
                .prepareGetAliases().setIndices("test_1").setIndicesOptions(IndicesOptions.strictExpand())::get,
                GetAliasesAction.NAME, "create_only");

        assertThrowsAuthorizationException(client().filterWithHeader(headers).admin().indices().prepareGetAliases("test_alias")
                .setIndices("test_*").setIndicesOptions(IndicesOptions.strictExpand())::get, GetAliasesAction.NAME, "create_only");

        //this throws exception no matter what the indices options are because the aliases part cannot be resolved to any alias
        //and there is no way to "allow_no_aliases" like we can do with indices.
        assertThrowsAuthorizationException(client().filterWithHeader(headers).admin().indices().prepareGetAliases()::get,
                GetAliasesAction.NAME, "create_only");
    }

    public void testGetAliasesCreateOnlyPermissionIgnoreUnavailable() {
        //user has create permission only: allows to create indices, manage_aliases is required to retrieve aliases though
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_only",
                new SecureString("test123".toCharArray())));

        assertThrowsAuthorizationException(client().filterWithHeader(headers).admin().indices().prepareGetAliases("test_1")
                .setIndices("test_1").setIndicesOptions(IndicesOptions.lenientExpandOpen())::get, GetAliasesAction.NAME, "create_only");

        assertThrowsAuthorizationException(client().filterWithHeader(headers).admin().indices().prepareGetAliases("_all")
                .setIndices("test_1").setIndicesOptions(IndicesOptions.lenientExpandOpen())::get, GetAliasesAction.NAME, "create_only");

        assertThrowsAuthorizationException(client().filterWithHeader(headers).admin().indices().prepareGetAliases().setIndices("test_1")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())::get, GetAliasesAction.NAME, "create_only");

        assertThrowsAuthorizationException(
                client().filterWithHeader(headers).admin().indices().prepareGetAliases("test_alias")
                .setIndices("test_*").setIndicesOptions(IndicesOptions.lenientExpandOpen())::get, GetAliasesAction.NAME, "create_only");

        //this throws exception no matter what the indices options are because the aliases part cannot be resolved to any alias
        //and there is no way to "allow_no_aliases" like we can do with indices.
        assertThrowsAuthorizationException(client().filterWithHeader(headers).admin().indices()
                .prepareGetAliases().setIndicesOptions(IndicesOptions.lenientExpandOpen())::get, GetAliasesAction.NAME, "create_only");
    }

    public void testCreateIndexThenAliasesCreateAndAliasesPermission() {
        //user has create and manage_aliases permission on test_*. manage_aliases is required to add/remove aliases on both aliases and
        // indices
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("create_test_aliases_test", new SecureString("test123".toCharArray())));

        assertAcked(client().filterWithHeader(headers).admin().indices().prepareCreate("test_1").get());

        //ok: user has manage_aliases on test_*
        assertAcked(client().filterWithHeader(headers).admin().indices().prepareAliases().addAlias("test_1", "test_alias").get());

        //ok: user has manage_aliases on test_*
        assertAcked(client().filterWithHeader(headers).admin().indices().prepareAliases().addAlias("test_*", "test_alias_2").get());

        //fails: user doesn't have manage_aliases on alias_1
        assertThrowsAuthorizationException(client().filterWithHeader(headers).admin().indices().prepareAliases()
                .addAlias("test_1", "alias_1").addAlias("test_1", "test_alias")::get,
                IndicesAliasesAction.NAME, "create_test_aliases_test");
    }

    public void testCreateIndexAndAliasesCreateAndAliasesPermission() {
        //user has create and manage_aliases permission on test_*. manage_aliases is required to add/remove aliases on both aliases and
        // indices
        //ok: user has manage_aliases on test_*
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test",
                new SecureString("test123".toCharArray())));
        assertAcked(client().filterWithHeader(headers).admin().indices().prepareCreate("test_1").addAlias(new Alias("test_alias")).get());

        //fails: user doesn't have manage_aliases on alias_1
        assertThrowsAuthorizationException(client().filterWithHeader(headers).admin().indices().prepareCreate("test_2")
                .addAlias(new Alias("test_alias")).addAlias(new Alias("alias_2"))::get,
                IndicesAliasesAction.NAME, "create_test_aliases_test");
    }

    public void testDeleteAliasesCreateAndAliasesPermission() {
        //user has create and manage_aliases permission on test_*. manage_aliases is required to add/remove aliases on both aliases and
        // indices
        //ok: user has manage_aliases on test_*
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("create_test_aliases_test",
                new SecureString("test123".toCharArray())));

        assertAcked(client().filterWithHeader(headers).admin().indices().prepareCreate("test_1").addAlias(new Alias("test_alias_1"))
                .addAlias(new Alias("test_alias_2"))
                .addAlias(new Alias("test_alias_3")).addAlias(new Alias("test_alias_4")).get());
        //ok: user has manage_aliases on test_*
        assertAcked(client().filterWithHeader(headers).admin().indices().prepareAliases().removeAlias("test_1", "test_alias_1").get());
        //ok: user has manage_aliases on test_*
        assertAcked(client().filterWithHeader(headers).admin().indices().prepareAliases().removeAlias("test_*", "test_alias_2").get());
        //ok: user has manage_aliases on test_*
        assertAcked(client().filterWithHeader(headers).admin().indices().prepareAliases().removeAlias("test_1", "test_alias_*").get());

        {
            //fails: all aliases have been deleted, no existing aliases match test_alias_*
            AliasesNotFoundException exception = expectThrows(AliasesNotFoundException.class,
                client().filterWithHeader(headers).admin().indices().prepareAliases().removeAlias("test_1", "test_alias_*")::get);
            assertThat(exception.getMessage(), equalTo("aliases [test_alias_*] missing"));
        }

        {
            //fails: all aliases have been deleted, no existing aliases match _all
            AliasesNotFoundException exception = expectThrows(AliasesNotFoundException.class,
                client().filterWithHeader(headers).admin().indices().prepareAliases().removeAlias("test_1", "_all")::get);
            assertThat(exception.getMessage(), equalTo("aliases [_all] missing"));
        }

        //fails: user doesn't have manage_aliases on alias_1
        assertThrowsAuthorizationException(client().filterWithHeader(headers).admin().indices().prepareAliases()
                .removeAlias("test_1", "alias_1")::get, IndicesAliasesAction.NAME, "create_test_aliases_test");

        //fails: user doesn't have manage_aliases on alias_1
        assertThrowsAuthorizationException(client().filterWithHeader(headers).admin().indices().prepareAliases()
                .removeAlias("test_1", new String[]{"_all", "alias_1"})::get, IndicesAliasesAction.NAME, "create_test_aliases_test");
    }

    public void testGetAliasesCreateAndAliasesPermission() {
        //user has create and manage_aliases permission on test_*. manage_aliases is required to retrieve aliases on both aliases and
        // indices
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("create_test_aliases_test", new SecureString("test123".toCharArray())));
        final Client client = client().filterWithHeader(headers);
        assertAcked(client.admin().indices().prepareCreate("test_1").addAlias(new Alias("test_alias")).get());

        //ok: user has manage_aliases on test_*
        assertAliases(client.admin().indices().prepareGetAliases().setAliases("test_alias").setIndices("test_1"),
                "test_1", "test_alias");

        //ok: user has manage_aliases on test_*, test_* gets resolved to test_1
        assertAliases(client.admin().indices().prepareGetAliases().setAliases("test_alias").setIndices("test_*"),
                "test_1", "test_alias");

        //ok: user has manage_aliases on test_*, empty indices gets resolved to _all indices (thus test_1)
        assertAliases(client.admin().indices().prepareGetAliases().setAliases("test_alias"),
                "test_1", "test_alias");

        //ok: user has manage_aliases on test_*, _all aliases gets resolved to test_alias and empty indices gets resolved to  _all
        // indices (thus test_1)
        assertAliases(client.admin().indices().prepareGetAliases().setAliases("_all").setIndices("test_1"),
                "test_1", "test_alias");

        //ok: user has manage_aliases on test_*, empty aliases gets resolved to test_alias and empty indices gets resolved to  _all
        // indices (thus test_1)
        assertAliases(client.admin().indices().prepareGetAliases().setIndices("test_1"),
                "test_1", "test_alias");

        //ok: user has manage_aliases on test_*, test_* aliases gets resolved to test_alias and empty indices gets resolved to  _all
        // indices (thus test_1)
        assertAliases(client.admin().indices().prepareGetAliases().setAliases("test_*").setIndices("test_1"),
                "test_1", "test_alias");

        //ok: user has manage_aliases on test_*, _all aliases gets resolved to test_alias and _all indices becomes test_1
        assertAliases(client.admin().indices().prepareGetAliases().setAliases("_all").setIndices("_all"),
                "test_1", "test_alias");

        //ok: user has manage_aliases on test_*, empty aliases gets resolved to test_alias and empty indices becomes test_1
        assertAliases(client.admin().indices().prepareGetAliases(),
                "test_1", "test_alias");

        //fails: user has manage_aliases on test_*, although _all aliases and empty indices can be resolved, the explicit non
        // authorized alias (alias_1) causes the request to fail
        assertThrowsAuthorizationException(client.admin().indices().prepareGetAliases().setAliases("_all", "alias_1")::get,
                GetAliasesAction.NAME, "create_test_aliases_test");

        //fails: user doesn't have manage_aliases on alias_1
        assertThrowsAuthorizationException(client.admin().indices().prepareGetAliases().setAliases("alias_1")::get,
                GetAliasesAction.NAME, "create_test_aliases_test");
    }

    public void testCreateIndexThenAliasesCreateAndAliasesPermission2() {
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("create_test_aliases_alias", new SecureString("test123".toCharArray())));
        final Client client = client().filterWithHeader(headers);

        //user has create permission on test_* and manage_aliases permission on alias_*. manage_aliases is required to add/remove aliases
        // on both aliases and indices
        assertAcked(client.admin().indices().prepareCreate("test_1"));

        //fails: user doesn't have manage_aliases aliases on test_1
        assertThrowsAuthorizationException(client.admin().indices().prepareAliases().addAlias("test_1", "test_alias")::get,
                IndicesAliasesAction.NAME, "create_test_aliases_alias");

        //fails: user doesn't have manage_aliases aliases on test_1
        assertThrowsAuthorizationException(client.admin().indices().prepareAliases().addAlias("test_1", "alias_1")::get,
                IndicesAliasesAction.NAME, "create_test_aliases_alias");

        //fails: user doesn't have manage_aliases aliases on test_*, no matching indices to replace wildcards
        IndexNotFoundException indexNotFoundException = expectThrows(IndexNotFoundException.class,
                client.admin().indices().prepareAliases().addAlias("test_*", "alias_1")::get);
        assertThat(indexNotFoundException.toString(), containsString("[test_*]"));
    }

    public void testCreateIndexAndAliasesCreateAndAliasesPermission2() {
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("create_test_aliases_alias", new
                SecureString("test123".toCharArray())));
        final Client client = client().filterWithHeader(headers);

        //user has create permission on test_* and manage_aliases permission on alias_*. manage_aliases is required to add/remove aliases
        // on both aliases and indices

        //fails: user doesn't have manage_aliases on test_1, create index is rejected as a whole
        assertThrowsAuthorizationException(client.admin().indices().prepareCreate("test_1").addAlias(new Alias("test_alias"))::get,
                IndicesAliasesAction.NAME, "create_test_aliases_alias");
    }

    public void testDeleteAliasesCreateAndAliasesPermission2() {
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("create_test_aliases_alias", new SecureString("test123".toCharArray())));
        final Client client = client().filterWithHeader(headers);

        //user has create permission on test_* and manage_aliases permission on alias_*. manage_aliases is required to add/remove aliases
        // on both aliases and indices

        //fails: user doesn't have manage_aliases on test_1
        assertThrowsAuthorizationException(client.admin().indices().prepareAliases().removeAlias("test_1", "test_alias")::get,
                IndicesAliasesAction.NAME, "create_test_aliases_alias");

        //fails: user doesn't have manage_aliases on test_*, wildcards can't get replaced
        expectThrows(IndexNotFoundException.class, client.admin().indices().prepareAliases().removeAlias("test_*", "alias_1")::get);
        }

    public void testGetAliasesCreateAndAliasesPermission2() {
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("create_test_aliases_alias", new SecureString("test123".toCharArray())));
        final Client client = client().filterWithHeader(headers);

        //user has create permission on test_* and manage_aliases permission on alias_*. manage_aliases is required to retrieve aliases
        // on both aliases and indices
        assertAcked(client.admin().indices().prepareCreate("test_1"));

        //fails: user doesn't have manage_aliases aliases on test_1, nor test_alias
        assertThrowsAuthorizationException(client.admin().indices().prepareGetAliases().setAliases("test_alias").setIndices("test_1")::get,
                GetAliasesAction.NAME, "create_test_aliases_alias");

        //user doesn't have manage_aliases aliases on test_*, no matching indices to replace wildcards
        GetAliasesResponse getAliasesResponse = client.admin().indices().prepareGetAliases()
                .setIndices("test_*").setAliases("test_alias").get();
        assertEquals(0, getAliasesResponse.getAliases().size());

        //no existing indices to replace empty indices (thus _all)
        getAliasesResponse = client.admin().indices().prepareGetAliases().setAliases("test_alias").get();
        assertEquals(0, getAliasesResponse.getAliases().size());

        {
            //fails: no existing aliases to replace wildcards
            assertThrowsAuthorizationException(
                client.admin().indices().prepareGetAliases().setIndices("test_1").setAliases("test_*")::get,
                GetAliasesAction.NAME, "create_test_aliases_alias");
        }
        {
            //fails: no existing aliases to replace _all
            assertThrowsAuthorizationException(client.admin().indices().prepareGetAliases().setIndices("test_1").setAliases("_all")::get,
                GetAliasesAction.NAME, "create_test_aliases_alias");
        }
        {
            //fails: no existing aliases to replace empty aliases
            assertThrowsAuthorizationException(client.admin().indices().prepareGetAliases().setIndices("test_1")::get,
                GetAliasesAction.NAME, "create_test_aliases_alias");
        }
        {
            //fails: no existing aliases to replace empty aliases
            GetAliasesResponse response = client.admin().indices().prepareGetAliases().get();
            assertThat(response.getAliases().size(), equalTo(0));
        }
    }

    public void testCreateIndexThenAliasesCreateAndAliasesPermission3() {
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("create_test_aliases_test_alias", new SecureString("test123".toCharArray())));
        final Client client = client().filterWithHeader(headers);

        //user has create permission on test_* and manage_aliases permission on test_*,alias_*. All good.
        assertAcked(client.admin().indices().prepareCreate("test_1"));

        assertAcked(client.admin().indices().prepareAliases().addAlias("test_1", "test_alias"));

        assertAcked(client.admin().indices().prepareAliases().addAlias("test_1", "alias_1"));

        assertAcked(client.admin().indices().prepareAliases().addAlias("test_*", "alias_2"));
    }

    public void testCreateIndexAndAliasesCreateAndAliasesPermission3() {
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("create_test_aliases_test_alias", new SecureString("test123".toCharArray())));
        final Client client = client().filterWithHeader(headers);

        //user has create permission on test_* and manage_aliases permission on test_*,alias_*. All good.
        assertAcked(client.admin().indices().prepareCreate("test_1").addAlias(new Alias("test_alias")));

        assertAcked(client.admin().indices().prepareCreate("test_2").addAlias(new Alias("test_alias_2")).addAlias(new Alias("alias_2")));
    }

    public void testDeleteAliasesCreateAndAliasesPermission3() {
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("create_test_aliases_test_alias", new SecureString("test123".toCharArray())));
        final Client client = client().filterWithHeader(headers);

        //user has create permission on test_* and manage_aliases permission on test_*,alias_*. All good.
        assertAcked(client.admin().indices().prepareCreate("test_1").addAlias(new Alias("test_alias")).addAlias(new Alias("alias_1"))
                .addAlias(new Alias("alias_2")).addAlias(new Alias("alias_3")));

        //fails: user doesn't have manage_aliases privilege on non_authorized
        assertThrowsAuthorizationException(client.admin().indices().prepareAliases().removeAlias("test_1", "non_authorized")
                .removeAlias("test_1", "test_alias")::get, IndicesAliasesAction.NAME, "create_test_aliases_test_alias");

        assertAcked(client.admin().indices().prepareAliases().removeAlias("test_1", "alias_1"));

        assertAcked(client.admin().indices().prepareAliases().removeAlias("test_*", "_all"));

        //fails: all aliases have been deleted, _all can't be resolved to any existing authorized aliases
        AliasesNotFoundException exception = expectThrows(AliasesNotFoundException.class,
                client.admin().indices().prepareAliases().removeAlias("test_1", "_all")::get);
        assertThat(exception.getMessage(), equalTo("aliases [_all] missing"));
    }

    public void testGetAliasesCreateAndAliasesPermission3() {
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("create_test_aliases_test_alias", new SecureString("test123".toCharArray())));
        final Client client = client().filterWithHeader(headers);

        //user has create permission on test_* and manage_aliases permission on test_*,alias_*. All good.
        assertAcked(client.admin().indices().prepareCreate("test_1").addAlias(new Alias("test_alias")).addAlias(new Alias("alias_1")));

        assertAliases(client.admin().indices().prepareGetAliases().setAliases("test_alias").setIndices("test_1"),
                "test_1", "test_alias");

        assertAliases(client.admin().indices().prepareGetAliases().setAliases("alias_1").setIndices("test_1"),
                "test_1", "alias_1");

        assertAliases(client.admin().indices().prepareGetAliases().setAliases("alias_1").setIndices("test_*"),
                "test_1", "alias_1");

        assertAliases(client.admin().indices().prepareGetAliases().setAliases("test_*").setIndices("test_1"),
                "test_1", "test_alias");

        assertAliases(client.admin().indices().prepareGetAliases().setAliases("_all").setIndices("test_1"),
                "test_1", "alias_1", "test_alias");

        assertAliases(client.admin().indices().prepareGetAliases().setAliases("_all"),
                "test_1", "alias_1", "test_alias");

        assertAliases(client.admin().indices().prepareGetAliases().setIndices("test_1"),
                "test_1", "alias_1", "test_alias");

        assertAliases(client.admin().indices().prepareGetAliases(), "test_1", "alias_1", "test_alias");

        assertAliases(client.admin().indices().prepareGetAliases().setAliases("alias_*").setIndices("test_*"),
                "test_1", "alias_1");
    }

    public void testCreateIndexAliasesOnlyPermission() {
        assertThrowsAuthorizationException(client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("aliases_only", new SecureString("test123".toCharArray()))))
                .admin().indices().prepareCreate("test_1")::get, CreateIndexAction.NAME, "aliases_only");
    }

    public void testGetAliasesAliasesOnlyPermissionStrict() {
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("aliases_only", new SecureString("test123".toCharArray())));
        final Client client = client().filterWithHeader(headers);
        //user has manage_aliases only permissions on both alias_* and test_*

        //security plugin lets it through, but es core intercepts it due to strict indices options and throws index not found
        IndexNotFoundException indexNotFoundException = expectThrows(IndexNotFoundException.class, client.admin().indices()
                .prepareGetAliases("alias_1").addIndices("test_1").setIndicesOptions(IndicesOptions.strictExpandOpen())::get);
        assertEquals("no such index [test_1]", indexNotFoundException.getMessage());

        //fails: no manage_aliases privilege on non_authorized alias
        assertThrowsAuthorizationException(client.admin().indices().prepareGetAliases("non_authorized").addIndices("test_1")
                .setIndicesOptions(IndicesOptions.strictExpandOpen())::get, GetAliasesAction.NAME, "aliases_only");

        //fails: no manage_aliases privilege on non_authorized index
        assertThrowsAuthorizationException(client.admin().indices().prepareGetAliases("alias_1").addIndices("non_authorized")
                .setIndicesOptions(IndicesOptions.strictExpandOpen())::get, GetAliasesAction.NAME, "aliases_only");
    }

    public void testGetAliasesAliasesOnlyPermissionIgnoreUnavailable() {
        Map<String, String> headers = Collections.singletonMap(BASIC_AUTH_HEADER,
                basicAuthHeaderValue("aliases_only", new SecureString("test123".toCharArray())));
        final Client client = client().filterWithHeader(headers);
        //user has manage_aliases only permissions on both alias_* and test_*

        //ok: manage_aliases on both test_* and alias_*
        GetAliasesResponse getAliasesResponse = client.admin().indices().prepareGetAliases("alias_1")
                .addIndices("test_1").setIndicesOptions(IndicesOptions.lenientExpandOpen()).get();
        assertEquals(0, getAliasesResponse.getAliases().size());

        //no manage_aliases privilege on non_authorized alias
        getAliasesResponse = client.admin().indices().prepareGetAliases("non_authorized").addIndices("test_1")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen()).get();
        assertEquals(0, getAliasesResponse.getAliases().size());

        //no manage_aliases privilege on non_authorized index
        getAliasesResponse = client.admin().indices().prepareGetAliases("alias_1").addIndices("non_authorized")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen()).get();
        assertEquals(0, getAliasesResponse.getAliases().size());
    }

    private static void assertAliases(GetAliasesRequestBuilder getAliasesRequestBuilder, String index, String... aliases) {
        GetAliasesResponse getAliasesResponse = getAliasesRequestBuilder.get();
        assertThat(getAliasesResponse.getAliases().size(), equalTo(1));
        assertThat(getAliasesResponse.getAliases().get(index).size(), equalTo(aliases.length));
        for (int i = 0; i < aliases.length; i++) {
            assertThat(getAliasesResponse.getAliases().get(index).get(i).alias(), equalTo(aliases[i]));
        }
    }
}
