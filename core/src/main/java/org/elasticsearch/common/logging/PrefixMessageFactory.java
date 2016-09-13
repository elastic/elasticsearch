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

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.MessageFactory2;
import org.apache.logging.log4j.message.ObjectMessage;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.message.SimpleMessage;

public class PrefixMessageFactory implements MessageFactory2 {

    private String prefix = "";

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Message newMessage(Object message) {
        return new PrefixObjectMessage(prefix, message);
    }

    private static class PrefixObjectMessage extends ObjectMessage {

        private final String prefix;
        private final Object object;
        private String prefixObjectString;

        private PrefixObjectMessage(String prefix, Object object) {
            super(object);
            this.prefix = prefix;
            this.object = object;
        }

        @Override
        public String getFormattedMessage() {
            if (prefixObjectString == null) {
                prefixObjectString = prefix + super.getFormattedMessage();
            }
            return prefixObjectString;
        }

        @Override
        public void formatTo(StringBuilder buffer) {
            buffer.append(prefix);
            super.formatTo(buffer);
        }

        @Override
        public Object[] getParameters() {
            return new Object[]{prefix, object};
        }

    }

    @Override
    public Message newMessage(String message) {
        return new PrefixSimpleMessage(prefix, message);
    }

    private static class PrefixSimpleMessage extends SimpleMessage {

        private final String prefix;
        private String prefixMessage;

        PrefixSimpleMessage(String prefix, String message) {
            super(message);
            this.prefix = prefix;
        }

        PrefixSimpleMessage(String prefix, CharSequence charSequence) {
            super(charSequence);
            this.prefix = prefix;
        }

        @Override
        public String getFormattedMessage() {
            if (prefixMessage == null) {
                prefixMessage = prefix + super.getFormattedMessage();
            }
            return prefixMessage;
        }

        @Override
        public void formatTo(StringBuilder buffer) {
            buffer.append(prefix);
            super.formatTo(buffer);
        }

        @Override
        public int length() {
            return prefixMessage.length();
        }

        @Override
        public char charAt(int index) {
            return prefixMessage.charAt(index);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return prefixMessage.subSequence(start, end);
        }

    }

    @Override
    public Message newMessage(String message, Object... params) {
        return new PrefixParameterizedMessage(prefix, message, params);
    }

    private static class PrefixParameterizedMessage extends ParameterizedMessage {

        private static ThreadLocal<StringBuilder> threadLocalStringBuilder = ThreadLocal.withInitial(StringBuilder::new);

        private final String prefix;
        private String formattedMessage;

        private PrefixParameterizedMessage(String prefix, String messagePattern, Object... arguments) {
            super(messagePattern, arguments);
            this.prefix = prefix;
        }

        @Override
        public String getFormattedMessage() {
            if (formattedMessage == null) {
                final StringBuilder buffer = threadLocalStringBuilder.get();
                buffer.setLength(0);
                formatTo(buffer);
                formattedMessage = buffer.toString();
            }
            return formattedMessage;
        }

        @Override
        public void formatTo(StringBuilder buffer) {
            buffer.append(prefix);
            super.formatTo(buffer);
        }

    }

    @Override
    public Message newMessage(CharSequence charSequence) {
        return new PrefixSimpleMessage(prefix, charSequence);
    }

    @Override
    public Message newMessage(String message, Object p0) {
        return new PrefixParameterizedMessage(prefix, message, p0);
    }

    @Override
    public Message newMessage(String message, Object p0, Object p1) {
        return new PrefixParameterizedMessage(prefix, message, p0, p1);
    }

    @Override
    public Message newMessage(String message, Object p0, Object p1, Object p2) {
        return new PrefixParameterizedMessage(prefix, message, p0, p1, p2);
    }

    @Override
    public Message newMessage(String message, Object p0, Object p1, Object p2, Object p3) {
        return new PrefixParameterizedMessage(prefix, message, p0, p1, p2, p3);
    }

    @Override
    public Message newMessage(String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        return new PrefixParameterizedMessage(prefix, message, p0, p1, p2, p3, p4);
    }

    @Override
    public Message newMessage(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5) {
        return new PrefixParameterizedMessage(prefix, message, p0, p1, p2, p3, p4, p5);
    }

    @Override
    public Message newMessage(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6) {
        return new PrefixParameterizedMessage(prefix, message, p0, p1, p2, p3, p4, p5, p6);
    }

    @Override
    public Message newMessage(String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7) {
        return new PrefixParameterizedMessage(prefix, message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    @Override
    public Message newMessage(
        String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8) {
        return new PrefixParameterizedMessage(prefix, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    @Override
    public Message newMessage(
        String message, Object p0, Object p1, Object p2, Object p3, Object p4, Object p5, Object p6, Object p7, Object p8, Object p9) {
        return new PrefixParameterizedMessage(prefix, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }
}
