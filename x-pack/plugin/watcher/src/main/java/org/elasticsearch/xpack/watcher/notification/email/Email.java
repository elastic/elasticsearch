/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.email;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.mail.MessagingException;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import static java.util.Collections.unmodifiableMap;

public class Email implements ToXContentObject {
    private static final DateFormatter DATE_TIME_FORMATTER = DateFormatter.forPattern("strict_date_time").withZone(ZoneOffset.UTC);
    final String id;
    final Address from;
    final AddressList replyTo;
    final Priority priority;
    final ZonedDateTime sentDate;
    final AddressList to;
    final AddressList cc;
    final AddressList bcc;
    final String subject;
    final String textBody;
    final String htmlBody;
    final Map<String, Attachment> attachments;

    public Email(
        String id,
        Address from,
        AddressList replyTo,
        Priority priority,
        ZonedDateTime sentDate,
        AddressList to,
        AddressList cc,
        AddressList bcc,
        String subject,
        String textBody,
        String htmlBody,
        Map<String, Attachment> attachments
    ) {

        this.id = id;
        this.from = from;
        this.replyTo = replyTo;
        this.priority = priority;
        this.sentDate = sentDate != null ? sentDate : ZonedDateTime.now(ZoneOffset.UTC);
        this.to = to;
        this.cc = cc;
        this.bcc = bcc;
        this.subject = subject;
        this.textBody = textBody;
        this.htmlBody = htmlBody;
        this.attachments = attachments;
    }

    public String id() {
        return id;
    }

    public Address from() {
        return from;
    }

    public AddressList replyTo() {
        return replyTo;
    }

    public Priority priority() {
        return priority;
    }

    public ZonedDateTime sentDate() {
        return sentDate;
    }

    public AddressList to() {
        return to;
    }

    public AddressList cc() {
        return cc;
    }

    public AddressList bcc() {
        return bcc;
    }

    public String subject() {
        return subject;
    }

    public String textBody() {
        return textBody;
    }

    public String htmlBody() {
        return htmlBody;
    }

    public Map<String, Attachment> attachments() {
        return attachments;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Field.ID.getPreferredName(), id);
        if (from != null) {
            builder.field(Field.FROM.getPreferredName(), from.toUnicodeString());
        }
        if (replyTo != null) {
            builder.field(Field.REPLY_TO.getPreferredName(), replyTo, params);
        }
        if (priority != null) {
            builder.field(Field.PRIORITY.getPreferredName(), priority.value());
        }
        builder.timestampField(Field.SENT_DATE.getPreferredName(), sentDate);
        if (to != null) {
            builder.field(Field.TO.getPreferredName(), to, params);
        }
        if (cc != null) {
            builder.field(Field.CC.getPreferredName(), cc, params);
        }
        if (bcc != null) {
            builder.field(Field.BCC.getPreferredName(), bcc, params);
        }
        builder.field(Field.SUBJECT.getPreferredName(), subject);
        if (textBody != null || htmlBody != null) {
            builder.startObject(Field.BODY.getPreferredName());
            if (textBody != null) {
                builder.field(Field.BODY_TEXT.getPreferredName(), textBody);
            }
            if (htmlBody != null) {
                builder.field(Field.BODY_HTML.getPreferredName(), htmlBody);
            }
            builder.endObject();
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Email email = (Email) o;

        if (id.equals(email.id) == false) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Email parse(XContentParser parser) throws IOException {
        Builder email = new Builder();
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if ((token.isValue() || token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY)
                && currentFieldName != null) {
                    if (Field.ID.match(currentFieldName, parser.getDeprecationHandler())) {
                        email.id(parser.text());
                    } else if (Field.FROM.match(currentFieldName, parser.getDeprecationHandler())) {
                        email.from(Address.parse(currentFieldName, token, parser));
                    } else if (Field.REPLY_TO.match(currentFieldName, parser.getDeprecationHandler())) {
                        email.replyTo(AddressList.parse(currentFieldName, token, parser));
                    } else if (Field.TO.match(currentFieldName, parser.getDeprecationHandler())) {
                        email.to(AddressList.parse(currentFieldName, token, parser));
                    } else if (Field.CC.match(currentFieldName, parser.getDeprecationHandler())) {
                        email.cc(AddressList.parse(currentFieldName, token, parser));
                    } else if (Field.BCC.match(currentFieldName, parser.getDeprecationHandler())) {
                        email.bcc(AddressList.parse(currentFieldName, token, parser));
                    } else if (Field.PRIORITY.match(currentFieldName, parser.getDeprecationHandler())) {
                        email.priority(Email.Priority.resolve(parser.text()));
                    } else if (Field.SENT_DATE.match(currentFieldName, parser.getDeprecationHandler())) {
                        email.sentDate(DateFormatters.from(DATE_TIME_FORMATTER.parse(parser.text())));
                    } else if (Field.SUBJECT.match(currentFieldName, parser.getDeprecationHandler())) {
                        email.subject(parser.text());
                    } else if (Field.BODY.match(currentFieldName, parser.getDeprecationHandler())) {
                        String bodyField = currentFieldName;
                        if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                            email.textBody(parser.text());
                        } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    currentFieldName = parser.currentName();
                                } else if (currentFieldName == null) {
                                    throw new ElasticsearchParseException("could not parse email. empty [{}] field", bodyField);
                                } else if (Email.Field.BODY_TEXT.match(currentFieldName, parser.getDeprecationHandler())) {
                                    email.textBody(parser.text());
                                } else if (Email.Field.BODY_HTML.match(currentFieldName, parser.getDeprecationHandler())) {
                                    email.htmlBody(parser.text());
                                } else {
                                    throw new ElasticsearchParseException(
                                        "could not parse email. unexpected field [{}.{}] field",
                                        bodyField,
                                        currentFieldName
                                    );
                                }
                            }
                        }
                    } else {
                        throw new ElasticsearchParseException("could not parse email. unexpected field [{}]", currentFieldName);
                    }
                }
        }
        return email.build();
    }

    public static class Builder {

        private String id;
        private Address from;
        private AddressList replyTo;
        private Priority priority;
        private ZonedDateTime sentDate;
        private AddressList to;
        private AddressList cc;
        private AddressList bcc;
        private String subject;
        private String textBody;
        private String htmlBody;
        private Map<String, Attachment> attachments = new HashMap<>();

        private Builder() {}

        public Builder copyFrom(Email email) {
            id = email.id;
            from = email.from;
            replyTo = email.replyTo;
            priority = email.priority;
            sentDate = email.sentDate;
            to = email.to;
            cc = email.cc;
            bcc = email.bcc;
            subject = email.subject;
            textBody = email.textBody;
            htmlBody = email.htmlBody;
            attachments.putAll(email.attachments);
            return this;
        }

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder from(String address) throws AddressException {
            return from(new Address(address));
        }

        public Builder from(Address from) {
            this.from = from;
            return this;
        }

        public Builder replyTo(AddressList replyTo) {
            this.replyTo = replyTo;
            return this;
        }

        public Builder replyTo(String addresses) throws AddressException {
            return replyTo(Email.AddressList.parse(addresses));
        }

        public Builder priority(Priority priority) {
            this.priority = priority;
            return this;
        }

        public Builder sentDate(ZonedDateTime sentDate) {
            this.sentDate = sentDate;
            return this;
        }

        public Builder to(String addresses) throws AddressException {
            return to(AddressList.parse(addresses));
        }

        public Builder to(AddressList to) {
            this.to = to;
            return this;
        }

        public AddressList to() {
            return to;
        }

        public Builder cc(String addresses) throws AddressException {
            return cc(AddressList.parse(addresses));
        }

        public Builder cc(AddressList cc) {
            this.cc = cc;
            return this;
        }

        public Builder bcc(String addresses) throws AddressException {
            return bcc(AddressList.parse(addresses));
        }

        public Builder bcc(AddressList bcc) {
            this.bcc = bcc;
            return this;
        }

        public Builder subject(String subject) {
            this.subject = subject;
            return this;
        }

        public Builder textBody(String text) {
            this.textBody = text;
            return this;
        }

        public Builder htmlBody(String html) {
            this.htmlBody = html;
            return this;
        }

        public Builder attach(Attachment attachment) {
            if (attachments == null) {
                throw new IllegalStateException("Email has already been built!");
            }
            attachments.put(attachment.id(), attachment);
            return this;
        }

        /**
         * Build the email. Note that adding items to attachments or inlines
         * after this is called is incorrect.
         */
        public Email build() {
            assert id != null : "email id should not be null";
            Email email = new Email(
                id,
                from,
                replyTo,
                priority,
                sentDate,
                to,
                cc,
                bcc,
                subject,
                textBody,
                htmlBody,
                unmodifiableMap(attachments)
            );
            attachments = null;
            return email;
        }

    }

    public enum Priority {

        HIGHEST(1),
        HIGH(2),
        NORMAL(3),
        LOW(4),
        LOWEST(5);

        static final String HEADER = "X-Priority";

        private final int value;

        Priority(int value) {
            this.value = value;
        }

        public void applyTo(MimeMessage message) throws MessagingException {
            message.setHeader(HEADER, String.valueOf(value));
        }

        public String value() {
            return name().toLowerCase(Locale.ROOT);
        }

        public static Priority resolve(String name) {
            Priority priority = resolve(name, null);
            if (priority == null) {
                throw new IllegalArgumentException("[" + name + "] is not a valid email priority");
            }
            return priority;
        }

        public static Priority resolve(String name, Priority defaultPriority) {
            if (name == null) {
                return defaultPriority;
            }
            return switch (name.toLowerCase(Locale.ROOT)) {
                case "highest" -> HIGHEST;
                case "high" -> HIGH;
                case "normal" -> NORMAL;
                case "low" -> LOW;
                case "lowest" -> LOWEST;
                default -> defaultPriority;
            };
        }

        public static Priority parse(Settings settings, String name) {
            String value = settings.get(name);
            if (value == null) {
                return null;
            }
            return resolve(value);
        }
    }

    public static class Address extends javax.mail.internet.InternetAddress implements ToXContentFragment {

        public static final ParseField ADDRESS_NAME_FIELD = new ParseField("name");
        public static final ParseField ADDRESS_EMAIL_FIELD = new ParseField("email");

        public Address(String address) throws AddressException {
            super(address);
        }

        public Address(String address, String personal) throws UnsupportedEncodingException {
            super(address, personal, StandardCharsets.UTF_8.name());
        }

        public static Address parse(String field, XContentParser.Token token, XContentParser parser) throws IOException {
            if (token == XContentParser.Token.VALUE_STRING) {
                String text = parser.text();
                try {
                    return new Email.Address(parser.text());
                } catch (AddressException ae) {
                    String msg = "could not parse [" + text + "] in field [" + field + "] as address. address must be RFC822 encoded";
                    throw new ElasticsearchParseException(msg, ae);
                }
            }

            if (token == XContentParser.Token.START_OBJECT) {
                String email = null;
                String name = null;
                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        if (ADDRESS_EMAIL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            email = parser.text();
                        } else if (ADDRESS_NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            name = parser.text();
                        } else {
                            throw new ElasticsearchParseException(
                                "could not parse [" + field + "] object as address. unknown address " + "field [" + currentFieldName + "]"
                            );
                        }
                    }
                }
                if (email == null) {
                    String msg = "could not parse [" + field + "] as address. address object must define an [email] field";
                    throw new ElasticsearchParseException(msg);
                }
                try {
                    return name != null ? new Email.Address(email, name) : new Email.Address(email);
                } catch (AddressException ae) {
                    throw new ElasticsearchParseException("could not parse [" + field + "] as address", ae);
                }

            }
            throw new ElasticsearchParseException(
                "could not parse [{}] as address. address must either be a string (RFC822 encoded) or "
                    + "an object specifying the address [name] and [email]",
                field
            );
        }

        public static Address parse(Settings settings, String name) {
            String value = settings.get(name);
            try {
                return value != null ? new Address(value) : null;
            } catch (AddressException ae) {
                throw new IllegalArgumentException("[" + value + "] is not a valid RFC822 email address", ae);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.value(toString());
        }
    }

    public static class AddressList implements Iterable<Address>, ToXContentObject {

        public static final AddressList EMPTY = new AddressList(Collections.<Address>emptyList());

        private final List<Address> addresses;

        public AddressList(List<Address> addresses) {
            this.addresses = addresses;
        }

        public boolean isEmpty() {
            return addresses.isEmpty();
        }

        @Override
        public Iterator<Address> iterator() {
            return addresses.iterator();
        }

        public Address[] toArray() {
            return addresses.toArray(new Address[addresses.size()]);
        }

        public int size() {
            return addresses.size();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray();
            for (Address address : addresses) {
                builder.value(address.toUnicodeString());
            }
            return builder.endArray();
        }

        public static AddressList parse(String text) throws AddressException {
            InternetAddress[] addresses = InternetAddress.parse(text);
            List<Address> list = new ArrayList<>(addresses.length);
            for (InternetAddress address : addresses) {
                list.add(new Address(address.toUnicodeString()));
            }
            return new AddressList(list);
        }

        public static AddressList parse(Settings settings, String name) {
            List<String> addresses = settings.getAsList(name);
            if (addresses == null || addresses.isEmpty()) {
                return null;
            }
            try {
                List<Address> list = new ArrayList<>(addresses.size());
                for (String address : addresses) {
                    list.add(new Address(address));
                }
                return new AddressList(list);
            } catch (AddressException ae) {
                throw new IllegalArgumentException("[" + settings.get(name) + "] is not a valid list of RFC822 email addresses", ae);
            }
        }

        public static Email.AddressList parse(String field, XContentParser.Token token, XContentParser parser) throws IOException {
            if (token == XContentParser.Token.VALUE_STRING) {
                String text = parser.text();
                try {
                    return parse(parser.text());
                } catch (AddressException ae) {
                    throw new ElasticsearchParseException(
                        "could not parse field ["
                            + field
                            + "] with value ["
                            + text
                            + "] as address "
                            + "list. address(es) must be RFC822 encoded",
                        ae
                    );
                }
            }
            if (token == XContentParser.Token.START_ARRAY) {
                List<Email.Address> addresses = new ArrayList<>();
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    addresses.add(Address.parse(field, token, parser));
                }
                return new Email.AddressList(addresses);
            }
            throw new ElasticsearchParseException(
                "could not parse ["
                    + field
                    + "] as address list. field must either be a string "
                    + "(comma-separated list of RFC822 encoded addresses) or an array of objects representing addresses"
            );
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            AddressList addresses1 = (AddressList) o;

            if (addresses.equals(addresses1.addresses) == false) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return addresses.hashCode();
        }
    }

    interface Field {
        ParseField ID = new ParseField("id");
        ParseField FROM = new ParseField("from");
        ParseField REPLY_TO = new ParseField("reply_to");
        ParseField PRIORITY = new ParseField("priority");
        ParseField SENT_DATE = new ParseField("sent_date");
        ParseField TO = new ParseField("to");
        ParseField CC = new ParseField("cc");
        ParseField BCC = new ParseField("bcc");
        ParseField SUBJECT = new ParseField("subject");
        ParseField BODY = new ParseField("body");
        ParseField BODY_TEXT = new ParseField("text");
        ParseField BODY_HTML = new ParseField("html");
    }

}
