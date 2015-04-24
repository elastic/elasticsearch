/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.actions.email.service.Authentication;
import org.elasticsearch.watcher.actions.email.service.Email;
import org.elasticsearch.watcher.actions.email.service.EmailTemplate;
import org.elasticsearch.watcher.actions.email.service.Profile;
import org.elasticsearch.watcher.support.secret.Secret;
import org.elasticsearch.watcher.support.secret.SensitiveXContentParser;
import org.elasticsearch.watcher.support.xcontent.WatcherParams;

import java.io.IOException;
import java.util.Locale;

/**
 *
 */
public class EmailAction implements Action {

    public static final String TYPE = "email";

    private final EmailTemplate email;
    private final @Nullable String account;
    private final @Nullable Authentication auth;
    private final @Nullable Profile profile;
    private final @Nullable Boolean attachData;

    public EmailAction(EmailTemplate email, @Nullable String account, @Nullable Authentication auth, @Nullable Profile profile, @Nullable Boolean attachData) {
        this.email = email;
        this.account = account;
        this.auth = auth;
        this.profile = profile;
        this.attachData = attachData;
    }

    public EmailTemplate getEmail() {
        return email;
    }

    public String getAccount() {
        return account;
    }

    public Authentication getAuth() {
        return auth;
    }

    public Profile getProfile() {
        return profile;
    }

    public boolean getAttachData() {
        return attachData != null && attachData;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EmailAction action = (EmailAction) o;

        if (!email.equals(action.email)) return false;
        if (account != null ? !account.equals(action.account) : action.account != null) return false;
        if (auth != null ? !auth.equals(action.auth) : action.auth != null) return false;
        if (profile != action.profile) return false;
        return !(attachData != null ? !attachData.equals(action.attachData) : action.attachData != null);
    }

    @Override
    public int hashCode() {
        int result = email.hashCode();
        result = 31 * result + (account != null ? account.hashCode() : 0);
        result = 31 * result + (auth != null ? auth.hashCode() : 0);
        result = 31 * result + (profile != null ? profile.hashCode() : 0);
        result = 31 * result + (attachData != null ? attachData.hashCode() : 0);
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (account != null) {
            builder.field(Field.ACCOUNT.getPreferredName(), account);
        }
        if (auth != null) {
            builder.field(Field.USER.getPreferredName(), auth.user());
            if (!WatcherParams.hideSecrets(params)) {
                builder.field(Field.PASSWORD.getPreferredName(), auth.password(), params);
            }
        }
        if (profile != null) {
            builder.field(Field.PROFILE.getPreferredName(), profile.name().toLowerCase(Locale.ROOT));
        }
        if (attachData != null) {
            builder.field(Field.ATTACH_DATA.getPreferredName(), attachData);
        }
        email.xContentBody(builder, params);
        return builder.endObject();
    }

    public static EmailAction parse(String watchId, String actionId, XContentParser parser) throws IOException {
        EmailTemplate.Parser emailParser = new EmailTemplate.Parser();
        String account = null;
        String user = null;
        Secret password = null;
        Profile profile = Profile.STANDARD;
        Boolean attachPayload = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (!emailParser.handle(currentFieldName, parser)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    if (Field.ACCOUNT.match(currentFieldName)) {
                        account = parser.text();
                    } else if (Field.USER.match(currentFieldName)) {
                        user = parser.text();
                    } else if (Field.PASSWORD.match(currentFieldName)) {
                        password = SensitiveXContentParser.secretOrNull(parser);
                    } else if (Field.PROFILE.match(currentFieldName)) {
                        profile = Profile.resolve(parser.text());
                    } else {
                        throw new EmailActionException("could not parse [{}] action [{}/{}]. unexpected string field [{}]", TYPE, watchId, actionId, currentFieldName);
                    }
                } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    if (Field.ATTACH_DATA.match(currentFieldName)) {
                        attachPayload = parser.booleanValue();
                    } else {
                        throw new EmailActionException("could not parse [{}] action [{}/{}]. unexpected boolean field [{}]", TYPE, watchId, actionId, currentFieldName);
                    }
                } else {
                    throw new EmailActionException("could not parse [{}] action [{}/{}]. unexpected token [{}]", TYPE, watchId, actionId, token);
                }
            }
        }

        Authentication auth = null;
        if (user != null) {
            auth = new Authentication(user, password);
        }

        return new EmailAction(emailParser.parsedTemplate(), account, auth, profile, attachPayload);
    }

    public static Builder builder(EmailTemplate email) {
        return new Builder(email);
    }

    public static abstract class Result extends Action.Result {

        protected Result(boolean success) {
            super(TYPE, success);
        }

        public static class Success extends Result {

            private final String account;
            private final Email email;

            Success(String account, Email email) {
                super(true);
                this.account = account;
                this.email = email;
            }

            public String account() {
                return account;
            }

            public Email email() {
                return email;
            }

            @Override
            public XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.field(Field.ACCOUNT.getPreferredName(), account)
                        .field(Field.EMAIL.getPreferredName(), email, params);
            }
        }

        public static class Failure extends Result {

            private final String reason;

            public Failure(String reason) {
                super(false);
                this.reason = reason;
            }

            public String reason() {
                return reason;
            }

            @Override
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.field(Action.Field.REASON.getPreferredName(), reason);
            }
        }

        public static class Simulated extends Result {

            private final Email email;

            public Email email() {
                return email;
            }

            Simulated(Email email) {
                super(true);
                this.email = email;
            }

            @Override
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.field(Field.SIMULATED_EMAIL.getPreferredName(), email, params);
            }
        }

        public static Result parse(String watchId, String actionId, XContentParser parser) throws IOException {
            Boolean success = null;
            Email email = null;
            Email simulatedEmail = null;
            String account = null;
            String reason = null;

            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (Field.EMAIL.match(currentFieldName)) {
                    try {
                        email = Email.parse(parser);
                    } catch (Email.ParseException pe) {
                        throw new EmailActionException("could not parse [{}] action result [{}/{}]. failed parsing [{}] field", pe, TYPE, watchId, actionId, currentFieldName);
                    }
                } else if (Field.SIMULATED_EMAIL.match(currentFieldName)) {
                    try {
                        simulatedEmail = Email.parse(parser);
                    } catch (Email.ParseException pe) {
                        throw new EmailActionException("could not parse [{}] action result [{}/{}]. failed parsing [{}] field", pe, TYPE, watchId, actionId, currentFieldName);
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (Field.ACCOUNT.match(currentFieldName)) {
                        account = parser.text();
                    } else if (Field.REASON.match(currentFieldName)) {
                        reason = parser.text();
                    } else {
                        throw new EmailActionException("could not parse [{}] action result [{}/{}]. unexpected string field [{}]", TYPE, watchId, actionId, currentFieldName);
                    }
                } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    if (Field.SUCCESS.match(currentFieldName)) {
                        success = parser.booleanValue();
                    } else {
                        throw new EmailActionException("could not parse [{}] action result [{}/{}]. unexpected boolean field [{}]", TYPE, watchId, actionId, currentFieldName);
                    }
                } else {
                    throw new EmailActionException("could not parse [{}] action result [{}/{}]. unexpected token [{}]", TYPE, watchId, actionId, token);
                }
            }

            if (simulatedEmail != null) {
                return new Simulated(simulatedEmail);
            }

            if (success == null) {
                throw new EmailActionException("could not parse [{}] action result [{}/{}]. missing required [{}] field", TYPE, watchId, actionId, Field.SUCCESS.getPreferredName());
            }

            if (success) {
                if (account == null) {
                    throw new EmailActionException("could not parse [{}] action successful result [{}/{}]. missing required [{}] field", TYPE, watchId, actionId, Field.ACCOUNT.getPreferredName());
                }
                if (email == null) {
                    throw new EmailActionException("could not parse [{}] action successful result [{}/{}]. missing required [{}] field", TYPE, watchId, actionId, Field.EMAIL.getPreferredName());
                }
                return new Success(account, email);
            }
            if (reason == null) {
                throw new EmailActionException("could not parse [{}] action failure result [{}/{}]. missing required [{}] field", TYPE, watchId, actionId, Field.REASON.getPreferredName());
            }
            return new Failure(reason);
        }
    }

    public static class Builder implements Action.Builder<EmailAction> {

        final EmailTemplate email;
        @Nullable String account;
        @Nullable Authentication auth;
        @Nullable Profile profile;
        @Nullable Boolean attachPayload;

        private Builder(EmailTemplate email) {
            this.email = email;
        }

        public Builder setAccount(String account) {
            this.account = account;
            return this;
        }

        public Builder setAuthentication(String username, char[] password) {
            this.auth = new Authentication(username, new Secret(password));
            return this;
        }

        public Builder setProfile(Profile profile) {
            this.profile = profile;
            return this;
        }

        public Builder setAttachPayload(boolean attachPayload) {
            this.attachPayload = attachPayload;
            return this;
        }

        public EmailAction build() {
            return new EmailAction(email, account, auth, profile, attachPayload);
        }
    }

    interface Field extends Action.Field {

        // common fields
        ParseField ACCOUNT = new ParseField("account");

        // action fields
        ParseField PROFILE = new ParseField("profile");
        ParseField USER = new ParseField("user");
        ParseField PASSWORD = new ParseField("password");
        ParseField ATTACH_DATA = new ParseField("attach_data");

        // result fields
        ParseField EMAIL = new ParseField("email");
        ParseField SIMULATED_EMAIL = new ParseField("simulated_email");
    }
}
