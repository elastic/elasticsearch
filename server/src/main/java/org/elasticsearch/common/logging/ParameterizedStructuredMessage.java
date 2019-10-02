package org.elasticsearch.common.logging;

import org.apache.logging.log4j.message.MapMessage;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.TriConsumer;

public class ParameterizedStructuredMessage extends MapMessage<ParameterizedStructuredMessage, Object> {

    private static final String MESSAGE = "message";
    private static final TriConsumer<String, Object, StringBuilder> ADD_KEY_VALUE_PAIR = new TriConsumer<>() {
        @Override
        public void accept(String key, Object value, StringBuilder builder) {
            if (!MESSAGE.equals(key)) {
                builder.append(' ').append(key).append('[').append(value).append(']');
            }
        }
    };

    private final String message;

    private ParameterizedStructuredMessage(String message) {
        this.message = message;
        with(MESSAGE, message);
    }

    private ParameterizedStructuredMessage(String messagePattern, Object... arguments) {
        this.message = ParameterizedMessage.format(messagePattern, arguments);
        with(MESSAGE, message);
    }

    public static ParameterizedStructuredMessage of(String message) {
        return new ParameterizedStructuredMessage(message);
    }

    public static ParameterizedStructuredMessage of(String messagePattern, Object... arguments) {
        return new ParameterizedStructuredMessage(messagePattern, arguments);
    }

    @Override
    protected void asJson(StringBuilder sb) {
        super.asJson(sb);
    }

    protected void appendMap(final StringBuilder sb) {
        sb.append(message);
        forEach(ADD_KEY_VALUE_PAIR, sb);
    }
}
