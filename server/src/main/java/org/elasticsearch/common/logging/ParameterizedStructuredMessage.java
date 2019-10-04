package org.elasticsearch.common.logging;

import org.apache.logging.log4j.message.MapMessage;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Chars;
import org.apache.logging.log4j.util.StringBuilders;
import org.apache.logging.log4j.util.Supplier;
import org.apache.logging.log4j.util.TriConsumer;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequestBuilder;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ParameterizedStructuredMessage extends MapMessage<ParameterizedStructuredMessage, Object> {

    private static final String MESSAGE = "message";
    private  Supplier<String> message;

    private String messagePattern;
    private List<Object> arguments = new ArrayList<>();

    public ParameterizedStructuredMessage(Supplier<String> message, Map<String, Object> map) {
        super(map);
        this.message = message;
    }

    public ParameterizedStructuredMessage(String messagePattern, Object... arguments) {
        super(new LinkedHashMap<>());
        this.messagePattern = messagePattern;
        Collections.addAll(this.arguments, arguments);
    }

    public static ParameterizedStructuredMessage of2(String messagePattern, Object... arguments){
       return new ParameterizedStructuredMessage(messagePattern, arguments);
    }

    public ParameterizedStructuredMessage with(String key, Object value) {
        this.arguments.add(value);
        super.with(key,value);
        return this;
    }

    public ParameterizedStructuredMessage field(String key, Object value) {
        super.with(key,value);
        return this;
    }


    public static ParameterizedStructuredMessageBuilder of(String messagePattern, Object... arguments) {
        return new ParameterizedStructuredMessageBuilder(messagePattern, arguments);
    }

    @Override
    protected void appendMap(final StringBuilder sb) {
        String message = ParameterizedMessage.format(messagePattern, arguments.toArray());
        sb.append(message);
    }

    //taken from super.asJson without the wrapping '{' '}'
    @Override
    protected void asJson(StringBuilder sb) {
        for (int i = 0; i < getIndexedReadOnlyStringMap().size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(Chars.DQUOTE);
            int start = sb.length();
            sb.append(getIndexedReadOnlyStringMap().getKeyAt(i));
            StringBuilders.escapeJson(sb, start);
            sb.append(Chars.DQUOTE).append(':').append(Chars.DQUOTE);
            start = sb.length();
            sb.append(getIndexedReadOnlyStringMap().getValueAt(i).toString());
//            ParameterFormatter.recursiveDeepToString(getIndexedReadOnlyStringMap().getValueAt(i), sb, null);
            StringBuilders.escapeJson(sb, start);
            sb.append(Chars.DQUOTE);
        }
    }

    public static class ParameterizedStructuredMessageBuilder {

        private String messagePattern;
        private List<Object> arguments = new ArrayList<>();
        private Map<String, Object> fields = new LinkedHashMap<>();

        public ParameterizedStructuredMessageBuilder(String messagePattern, Object[] arguments) {
            this.messagePattern = messagePattern;
            Collections.addAll(this.arguments, arguments);
        }

        public ParameterizedStructuredMessageBuilder with(String key, Object value) {
            this.arguments.add(value);
            fields.put(key, value);
            return this;
        }

        public ParameterizedStructuredMessageBuilder field(String key, Object value) {
            fields.put(key, value);
            return this;
        }

        public ParameterizedStructuredMessage build() {
            Supplier<String> messageSupplier = () -> ParameterizedMessage.format(messagePattern, arguments.toArray());

//            if(fields.containsKey(MESSAGE) == false){
//                with(MESSAGE, new Object(){
//                    @Override
//                    public String toString() {
//                        return messageSupplier.get();
//                    }
//                });
//
//
//            }
            return new ParameterizedStructuredMessage(messageSupplier, fields);
        }
    }
}
