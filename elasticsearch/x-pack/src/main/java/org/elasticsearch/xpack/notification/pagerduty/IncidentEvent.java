/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.pagerduty;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.support.http.HttpMethod;
import org.elasticsearch.watcher.support.http.HttpRequest;
import org.elasticsearch.watcher.support.http.Scheme;
import org.elasticsearch.watcher.support.text.TextTemplate;
import org.elasticsearch.watcher.support.text.TextTemplateEngine;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Official documentation for this can be found at
 *
 * https://developer.pagerduty.com/documentation/howto/manually-trigger-an-incident/
 * https://developer.pagerduty.com/documentation/integration/events/trigger
 * https://developer.pagerduty.com/documentation/integration/events/acknowledge
 * https://developer.pagerduty.com/documentation/integration/events/resolve
 */
public class IncidentEvent implements ToXContent {

    static final String HOST = "events.pagerduty.com";
    static final String PATH = "/generic/2010-04-15/create_event.json";

    final String description;
    final @Nullable String incidentKey;
    final @Nullable String client;
    final @Nullable String clientUrl;
    final @Nullable String account;
    final String eventType;
    final boolean attachPayload;
    final @Nullable IncidentEventContext[] contexts;

    public IncidentEvent(String description, @Nullable String eventType, @Nullable String incidentKey, @Nullable String client,
                         @Nullable String clientUrl, @Nullable String account, boolean attachPayload,
                         @Nullable IncidentEventContext[] contexts) {
        this.description = description;
        if (description == null) {
            throw new IllegalStateException("could not create pagerduty event. missing required [" +
                    XField.DESCRIPTION.getPreferredName() + "] setting");
        }
        this.incidentKey = incidentKey;
        this.client = client;
        this.clientUrl = clientUrl;
        this.account = account;
        this.attachPayload = attachPayload;
        this.contexts = contexts;
        this.eventType = Strings.hasLength(eventType) ? eventType : "trigger";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IncidentEvent template = (IncidentEvent) o;
        return Objects.equals(description, template.description) &&
                Objects.equals(incidentKey, template.incidentKey) &&
                Objects.equals(client, template.client) &&
                Objects.equals(clientUrl, template.clientUrl) &&
                Objects.equals(attachPayload, template.attachPayload) &&
                Objects.equals(eventType, template.eventType) &&
                Objects.equals(account, template.account) &&
                Arrays.equals(contexts, template.contexts);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(description, incidentKey, client, clientUrl, account, attachPayload, eventType);
        result = 31 * result + Arrays.hashCode(contexts);
        return result;
    }

    public HttpRequest createRequest(final String serviceKey, final Payload payload) throws IOException {
        return HttpRequest.builder(HOST, -1)
                .method(HttpMethod.POST)
                .scheme(Scheme.HTTPS)
                .path(PATH)
                .jsonBody(new ToXContent() {
                    @Override
                    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                        builder.field(XField.SERVICE_KEY.getPreferredName(), serviceKey);
                        builder.field(XField.EVENT_TYPE.getPreferredName(), eventType);
                        builder.field(XField.DESCRIPTION.getPreferredName(), description);
                        if (incidentKey != null) {
                            builder.field(XField.INCIDENT_KEY.getPreferredName(), incidentKey);
                        }
                        if (client != null) {
                            builder.field(XField.CLIENT.getPreferredName(), client);
                        }
                        if (clientUrl != null) {
                            builder.field(XField.CLIENT_URL.getPreferredName(), clientUrl);
                        }
                        if (attachPayload) {
                            builder.startObject(XField.DETAILS.getPreferredName());
                            builder.field(XField.PAYLOAD.getPreferredName());
                            payload.toXContent(builder, params);
                            builder.endObject();
                        }
                        if (contexts != null && contexts.length > 0) {
                            builder.startArray(IncidentEvent.XField.CONTEXT.getPreferredName());
                            for (IncidentEventContext context : contexts) {
                                context.toXContent(builder, params);
                            }
                            builder.endArray();
                        }
                        return builder;
                    }
                })
                .build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(XField.DESCRIPTION.getPreferredName(), description);
        if (incidentKey != null) {
            builder.field(XField.INCIDENT_KEY.getPreferredName(), incidentKey);
        }
        if (client != null) {
            builder.field(XField.CLIENT.getPreferredName(), client);
        }
        if (clientUrl != null) {
            builder.field(XField.CLIENT_URL.getPreferredName(), clientUrl);
        }
        if (account != null) {
            builder.field(XField.ACCOUNT.getPreferredName(), account);
        }
        builder.field(XField.ATTACH_PAYLOAD.getPreferredName(), attachPayload);
        if (contexts != null) {
            builder.startArray(XField.CONTEXT.getPreferredName());
            for (IncidentEventContext context : contexts) {
                context.toXContent(builder, params);
            }
            builder.endArray();
        }
        return builder.endObject();
    }
    public static Template.Builder templateBuilder(String description) {
        return templateBuilder(TextTemplate.inline(description).build());
    }

    public static Template.Builder templateBuilder(TextTemplate description) {
        return new Template.Builder(description);
    }

    public static class Template implements ToXContent {

        final TextTemplate description;
        final TextTemplate incidentKey;
        final TextTemplate client;
        final TextTemplate clientUrl;
        final TextTemplate eventType;
        public final String account;
        final Boolean attachPayload;
        final IncidentEventContext.Template[] contexts;

        public Template(TextTemplate description, TextTemplate eventType, TextTemplate incidentKey, TextTemplate client,
                        TextTemplate clientUrl, String account, Boolean attachPayload, IncidentEventContext.Template[] contexts) {
            this.description = description;
            this.eventType = eventType;
            this.incidentKey = incidentKey;
            this.client = client;
            this.clientUrl = clientUrl;
            this.account = account;
            this.attachPayload = attachPayload;
            this.contexts = contexts;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Template template = (Template) o;
            return Objects.equals(description, template.description) &&
                   Objects.equals(incidentKey, template.incidentKey) &&
                   Objects.equals(client, template.client) &&
                   Objects.equals(clientUrl, template.clientUrl) &&
                   Objects.equals(eventType, template.eventType) &&
                   Objects.equals(attachPayload, template.attachPayload) &&
                   Objects.equals(account, template.account) &&
                   Arrays.equals(contexts, template.contexts);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(description, eventType, incidentKey, client, clientUrl, attachPayload, account);
            result = 31 * result + Arrays.hashCode(contexts);
            return result;
        }

        public IncidentEvent render(String watchId, String actionId, TextTemplateEngine engine, Map<String, Object> model,
                                    IncidentEventDefaults defaults) {
            String description = this.description != null ? engine.render(this.description, model) : defaults.description;
            String incidentKey = this.incidentKey != null ? engine.render(this.incidentKey, model) :
                    defaults.incidentKey != null ? defaults.incidentKey : watchId;
            String client = this.client != null ? engine.render(this.client, model) : defaults.client;
            String clientUrl = this.clientUrl != null ? engine.render(this.clientUrl, model) : defaults.clientUrl;
            String eventType = this.eventType != null ? engine.render(this.eventType, model) : defaults.eventType;
            boolean attachPayload = this.attachPayload != null ? this.attachPayload : defaults.attachPayload;
            IncidentEventContext[] contexts = null;
            if (this.contexts != null) {
                contexts = new IncidentEventContext[this.contexts.length];
                for (int i = 0; i < this.contexts.length; i++) {
                    contexts[i] = this.contexts[i].render(engine, model, defaults);
                }
            }
            return new IncidentEvent(description, eventType, incidentKey, client, clientUrl, account, attachPayload, contexts);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field(XField.DESCRIPTION.getPreferredName(), description, params);
            if (incidentKey != null) {
                builder.field(XField.INCIDENT_KEY.getPreferredName(), incidentKey, params);
            }
            if (client != null) {
                builder.field(XField.CLIENT.getPreferredName(), client, params);
            }
            if (clientUrl != null) {
                builder.field(XField.CLIENT_URL.getPreferredName(), clientUrl, params);
            }
            if (eventType != null) {
                builder.field(XField.EVENT_TYPE.getPreferredName(), eventType, params);
            }
            if (attachPayload != null) {
                builder.field(XField.ATTACH_PAYLOAD.getPreferredName(), attachPayload);
            }
            if (account != null) {
                builder.field(XField.ACCOUNT.getPreferredName(), account);
            }
            if (contexts != null) {
                builder.startArray(XField.CONTEXT.getPreferredName());
                for (IncidentEventContext.Template context : contexts) {
                    context.toXContent(builder, params);
                }
                builder.endArray();
            }
            return builder.endObject();
        }

        public static Template parse(String watchId, String actionId, XContentParser parser) throws IOException {
            TextTemplate incidentKey = null;
            TextTemplate description = null;
            TextTemplate client = null;
            TextTemplate clientUrl = null;
            TextTemplate eventType = null;
            String account = null;
            Boolean attachPayload = null;
            IncidentEventContext.Template[] contexts = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, XField.INCIDENT_KEY)) {
                    try {
                        incidentKey = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException e) {
                        throw new ElasticsearchParseException("could not parse pager duty event template. failed to parse field [{}]",
                                XField.INCIDENT_KEY.getPreferredName());
                    }
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, XField.DESCRIPTION)) {
                    try {
                        description = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException e) {
                        throw new ElasticsearchParseException("could not parse pager duty event template. failed to parse field [{}]",
                                XField.DESCRIPTION.getPreferredName());
                    }
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, XField.CLIENT)) {
                    try {
                        client = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException e) {
                        throw new ElasticsearchParseException("could not parse pager duty event template. failed to parse field [{}]",
                                XField.CLIENT.getPreferredName());
                    }
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, XField.CLIENT_URL)) {
                    try {
                        clientUrl = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException e) {
                        throw new ElasticsearchParseException("could not parse pager duty event template. failed to parse field [{}]",
                                XField.CLIENT_URL.getPreferredName());
                    }
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, XField.ACCOUNT)) {
                    try {
                        account = parser.text();
                    } catch (ElasticsearchParseException e) {
                        throw new ElasticsearchParseException("could not parse pager duty event template. failed to parse field [{}]",
                                XField.CLIENT_URL.getPreferredName());
                    }
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, XField.EVENT_TYPE)) {
                    try {
                        eventType = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException e) {
                        throw new ElasticsearchParseException("could not parse pager duty event template. failed to parse field [{}]",
                                XField.EVENT_TYPE.getPreferredName());
                    }
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, XField.ATTACH_PAYLOAD)) {
                    if (token == XContentParser.Token.VALUE_BOOLEAN) {
                        attachPayload = parser.booleanValue();
                    } else {
                        throw new ElasticsearchParseException("could not parse pager duty event template. failed to parse field [{}], " +
                                "expected a boolean value but found [{}] instead", XField.ATTACH_PAYLOAD.getPreferredName(), token);
                    }
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, XField.CONTEXT)) {
                    if (token == XContentParser.Token.START_ARRAY) {
                        List<IncidentEventContext.Template> list = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            try {
                                list.add(IncidentEventContext.Template.parse(parser));
                            } catch (ElasticsearchParseException e) {
                                throw new ElasticsearchParseException("could not parse pager duty event template. failed to parse field " +
                                        "[{}]", XField.CONTEXT.getPreferredName());
                            }
                        }
                        contexts = list.toArray(new IncidentEventContext.Template[list.size()]);
                    }
                } else {
                    throw new ElasticsearchParseException("could not parse pager duty event template. unexpected field [{}]",
                            currentFieldName);
                }
            }
            return new Template(description, eventType, incidentKey, client, clientUrl, account, attachPayload, contexts);
        }

        public static class Builder {

            final TextTemplate description;
            TextTemplate incidentKey;
            TextTemplate client;
            TextTemplate clientUrl;
            TextTemplate eventType;
            String account;
            Boolean attachPayload;
            List<IncidentEventContext.Template> contexts = new ArrayList<>();

            public Builder(TextTemplate description) {
                this.description = description;
            }

            public Builder setIncidentKey(TextTemplate incidentKey) {
                this.incidentKey = incidentKey;
                return this;
            }

            public Builder setClient(TextTemplate client) {
                this.client = client;
                return this;
            }

            public Builder setClientUrl(TextTemplate clientUrl) {
                this.clientUrl = clientUrl;
                return this;
            }

            public Builder setEventType(TextTemplate eventType) {
                this.eventType = eventType;
                return this;
            }

            public Builder setAccount(String account) {
                this.account= account;
                return this;
            }

            public Builder setAttachPayload(Boolean attachPayload) {
                this.attachPayload = attachPayload;
                return this;
            }

            public Builder addContext(IncidentEventContext.Template context) {
                this.contexts.add(context);
                return this;
            }

            public Template build() {
                IncidentEventContext.Template[] contexts = this.contexts.isEmpty() ? null :
                        this.contexts.toArray(new IncidentEventContext.Template[this.contexts.size()]);
                return new Template(description, eventType, incidentKey, client, clientUrl, account, attachPayload, contexts);
            }
        }
    }

    interface XField {

        ParseField TYPE = new ParseField("type");
        ParseField EVENT_TYPE = new ParseField("event_type");

        ParseField ACCOUNT = new ParseField("account");
        ParseField DESCRIPTION = new ParseField("description");
        ParseField INCIDENT_KEY = new ParseField("incident_key");
        ParseField CLIENT = new ParseField("client");
        ParseField CLIENT_URL = new ParseField("client_url");
        ParseField ATTACH_PAYLOAD = new ParseField("attach_payload");
        ParseField CONTEXT = new ParseField("context");

        ParseField SERVICE_KEY = new ParseField("service_key");
        ParseField PAYLOAD = new ParseField("payload");
        ParseField DETAILS = new ParseField("details");
    }
}
