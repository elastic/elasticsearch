/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.test;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.alerts.actions.Action;
import org.elasticsearch.alerts.actions.Actions;
import org.elasticsearch.alerts.actions.email.EmailAction;
import org.elasticsearch.alerts.actions.email.service.Authentication;
import org.elasticsearch.alerts.actions.email.service.Email;
import org.elasticsearch.alerts.actions.email.service.EmailService;
import org.elasticsearch.alerts.actions.email.service.Profile;
import org.elasticsearch.alerts.actions.webhook.HttpClient;
import org.elasticsearch.alerts.actions.webhook.WebhookAction;
import org.elasticsearch.alerts.condition.script.ScriptCondition;
import org.elasticsearch.alerts.input.search.SearchInput;
import org.elasticsearch.alerts.scheduler.schedule.CronSchedule;
import org.elasticsearch.alerts.support.AlertUtils;
import org.elasticsearch.alerts.support.Script;
import org.elasticsearch.alerts.support.init.proxy.ClientProxy;
import org.elasticsearch.alerts.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.alerts.support.template.ScriptTemplate;
import org.elasticsearch.alerts.support.template.Template;
import org.elasticsearch.alerts.transform.SearchTransform;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.netty.handler.codec.http.HttpMethod;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import javax.mail.internet.AddressException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public final class AlertsTestUtils {

    public static final Payload EMPTY_PAYLOAD = new Payload.Simple(ImmutableMap.<String, Object>of());

    private AlertsTestUtils() {
    }

    public static SearchRequest newInputSearchRequest(String... indices) {
        SearchRequest request = new SearchRequest(indices);
        request.indicesOptions(AlertUtils.DEFAULT_INDICES_OPTIONS);
        request.searchType(SearchInput.DEFAULT_SEARCH_TYPE);
        return request;
    }

    public static SearchRequest matchAllRequest() {
        return matchAllRequest(null);
    }

    public static SearchRequest matchAllRequest(IndicesOptions indicesOptions) {
        SearchRequest request = new SearchRequest(Strings.EMPTY_ARRAY)
                .source(SearchSourceBuilder.searchSource().query(matchAllQuery()).buildAsBytes(XContentType.JSON), false);
        if (indicesOptions != null) {
            request.indicesOptions(indicesOptions);
        }
        return request;
    }

    public static Payload simplePayload(String key, Object value) {
        return new Payload.Simple(key, value);
    }

    public static ExecutionContext mockExecutionContext(String alertName, Payload payload) {
        DateTime now = DateTime.now();
        return mockExecutionContext(now, now, alertName, payload);
    }

    public static ExecutionContext mockExecutionContext(DateTime scheduledTime, DateTime firedTime, String alertName, Payload payload) {
        ExecutionContext ctx = mock(ExecutionContext.class);
        when(ctx.scheduledTime()).thenReturn(scheduledTime);
        when(ctx.fireTime()).thenReturn(firedTime);
        Alert alert = mock(Alert.class);
        when(alert.name()).thenReturn(alertName);
        when(ctx.alert()).thenReturn(alert);
        when(ctx.payload()).thenReturn(payload);
        return ctx;
    }

    public static Alert createTestAlert(String alertName, ScriptServiceProxy scriptService, HttpClient httpClient, EmailService emailService, ESLogger logger) throws AddressException {
        SearchRequest conditionRequest = newInputSearchRequest("my-condition-index").source(searchSource().query(matchAllQuery()));
        SearchRequest transformRequest = newInputSearchRequest("my-payload-index").source(searchSource().query(matchAllQuery()));
        transformRequest.searchType(SearchTransform.DEFAULT_SEARCH_TYPE);
        conditionRequest.searchType(SearchInput.DEFAULT_SEARCH_TYPE);

        List<Action> actions = new ArrayList<>();

        Template url = new ScriptTemplate(scriptService, "http://localhost/foobarbaz/{{alert_name}}");
        Template body = new ScriptTemplate(scriptService, "{{alert_name}} executed with {{response.hits.total}} hits");

        actions.add(new WebhookAction(logger, null, httpClient, HttpMethod.GET, url, body));

        Email.Address from = new Email.Address("from@test.com");
        List<Email.Address> emailAddressList = new ArrayList<>();
        emailAddressList.add(new Email.Address("to@test.com"));
        Email.AddressList to = new Email.AddressList(emailAddressList);


        Email.Builder emailBuilder = Email.builder().id("prototype");
        emailBuilder.from(from);
        emailBuilder.to(to);


        EmailAction emailAction = new EmailAction(logger, null, emailService, emailBuilder.build(),
                new Authentication("testname", "testpassword"), Profile.STANDARD, "testaccount", body, body, null, true);

        actions.add(emailAction);

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("foo", "bar");

        return new Alert(
                alertName,
                new CronSchedule("0/5 * * * * ? *"),
                new SearchInput(logger, scriptService, ClientProxy.of(ElasticsearchIntegrationTest.client()),
                        conditionRequest),
                new ScriptCondition(logger, scriptService, new Script("return true")),
                new SearchTransform(logger, scriptService, ClientProxy.of(ElasticsearchIntegrationTest.client()), transformRequest),
                new Actions(actions),
                metadata,
                new TimeValue(0),
                new Alert.Status());
    }
}
