package org.elasticsearch.rest.action.cat;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;

/**
 * Tests for {@link RestAliasAction}
 */
public class RestAliasActionTests extends ESTestCase {

    /** Instance of tested Action */
    private RestAliasAction action;
    /** Instance of fake node client */
    private NodeClient fakeClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        action = new RestAliasAction(Settings.EMPTY, new RestController(Settings.EMPTY, Collections.emptySet(), null, null, null));

        fakeClient = new NodeClient(Settings.EMPTY, new ThreadPool(Settings.EMPTY));
        fakeClient.admin().indices().prepareCreate("foo").addAlias(new Alias("alias_1")).addAlias(new Alias("alias_2")).get();
        fakeClient.admin().indices().prepareCreate("bar").addAlias(new Alias("alias_1")).get();
    }


    public void testSimpleAlias(){
        RestRequest fakeReq = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET).withPath("_cat/aliases/alias_1?v").build();
        action.doCatRequest(fakeReq, fakeClient).accept();
    }

    public void testFailMultipleAlias(){

    }

    public void testMultipleAlias(){

    }

}
