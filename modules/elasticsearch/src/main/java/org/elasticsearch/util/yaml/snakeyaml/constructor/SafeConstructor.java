/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.elasticsearch.util.yaml.snakeyaml.constructor;

import org.elasticsearch.util.yaml.snakeyaml.error.YAMLException;
import org.elasticsearch.util.yaml.snakeyaml.nodes.*;
import org.elasticsearch.util.yaml.snakeyaml.util.Base64Coder;

import java.math.BigInteger;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Construct standard Java classes
 *
 * @see <a href="http://pyyaml.org/wiki/PyYAML">PyYAML</a> for more information
 */
public class SafeConstructor extends BaseConstructor {

    public static ConstructUndefined undefinedConstructor = new ConstructUndefined();

    public SafeConstructor() {
        this.yamlConstructors.put(Tag.NULL, new ConstructYamlNull());
        this.yamlConstructors.put(Tag.BOOL, new ConstructYamlBool());
        this.yamlConstructors.put(Tag.INT, new ConstructYamlInt());
        this.yamlConstructors.put(Tag.FLOAT, new ConstructYamlFloat());
        this.yamlConstructors.put(Tag.BINARY, new ConstructYamlBinary());
        this.yamlConstructors.put(Tag.TIMESTAMP, new ConstructYamlTimestamp());
        this.yamlConstructors.put(Tag.OMAP, new ConstructYamlOmap());
        this.yamlConstructors.put(Tag.PAIRS, new ConstructYamlPairs());
        this.yamlConstructors.put(Tag.SET, new ConstructYamlSet());
        this.yamlConstructors.put(Tag.STR, new ConstructYamlStr());
        this.yamlConstructors.put(Tag.SEQ, new ConstructYamlSeq());
        this.yamlConstructors.put(Tag.MAP, new ConstructYamlMap());
        this.yamlConstructors.put(null, undefinedConstructor);
        this.yamlClassConstructors.put(NodeId.scalar, undefinedConstructor);
        this.yamlClassConstructors.put(NodeId.sequence, undefinedConstructor);
        this.yamlClassConstructors.put(NodeId.mapping, undefinedConstructor);
    }

    private void flattenMapping(MappingNode node) {
        List<NodeTuple> merge = new ArrayList<NodeTuple>();
        int index = 0;
        List<NodeTuple> nodeValue = (List<NodeTuple>) node.getValue();
        while (index < nodeValue.size()) {
            YamlNode keyNode = nodeValue.get(index).getKeyNode();
            YamlNode valueNode = nodeValue.get(index).getValueNode();
            if (keyNode.getTag().equals(Tag.MERGE)) {
                nodeValue.remove(index);
                switch (valueNode.getNodeId()) {
                    case mapping:
                        MappingNode mn = (MappingNode) valueNode;
                        flattenMapping(mn);
                        merge.addAll(mn.getValue());
                        break;
                    case sequence:
                        List<List<NodeTuple>> submerge = new ArrayList<List<NodeTuple>>();
                        SequenceNode sn = (SequenceNode) valueNode;
                        List<YamlNode> vals = sn.getValue();
                        for (YamlNode subnode : vals) {
                            if (!(subnode instanceof MappingNode)) {
                                throw new ConstructorException("while constructing a mapping", node
                                        .getStartMark(), "expected a mapping for merging, but found "
                                        + subnode.getNodeId(), subnode.getStartMark());
                            }
                            MappingNode mnode = (MappingNode) subnode;
                            flattenMapping(mnode);
                            submerge.add(mnode.getValue());
                        }
                        Collections.reverse(submerge);
                        for (List<NodeTuple> value : submerge) {
                            merge.addAll(value);
                        }
                        break;
                    default:
                        throw new ConstructorException("while constructing a mapping", node
                                .getStartMark(),
                                "expected a mapping or list of mappings for merging, but found "
                                        + valueNode.getNodeId(), valueNode.getStartMark());
                }
            } else if (keyNode.getTag().equals(Tag.VALUE)) {
                keyNode.setTag(Tag.STR);
                index++;
            } else {
                index++;
            }
        }
        if (!merge.isEmpty()) {
            merge.addAll(nodeValue);
            ((MappingNode) node).setValue(merge);
        }
    }

    protected void constructMapping2ndStep(MappingNode node, Map<Object, Object> mapping) {
        flattenMapping(node);
        super.constructMapping2ndStep(node, mapping);
    }

    @Override
    protected void constructSet2ndStep(MappingNode node, java.util.Set<Object> set) {
        flattenMapping(node);
        super.constructSet2ndStep(node, set);
    }

    private class ConstructYamlNull extends AbstractConstruct {
        public Object construct(YamlNode node) {
            constructScalar((ScalarNode) node);
            return null;
        }
    }

    private final static Map<String, Boolean> BOOL_VALUES = new HashMap<String, Boolean>();

    static {
        BOOL_VALUES.put("yes", Boolean.TRUE);
        BOOL_VALUES.put("no", Boolean.FALSE);
        BOOL_VALUES.put("true", Boolean.TRUE);
        BOOL_VALUES.put("false", Boolean.FALSE);
        BOOL_VALUES.put("on", Boolean.TRUE);
        BOOL_VALUES.put("off", Boolean.FALSE);
    }

    private class ConstructYamlBool extends AbstractConstruct {
        public Object construct(YamlNode node) {
            String val = (String) constructScalar((ScalarNode) node);
            return BOOL_VALUES.get(val.toLowerCase());
        }
    }

    private class ConstructYamlInt extends AbstractConstruct {
        public Object construct(YamlNode node) {
            String value = constructScalar((ScalarNode) node).toString().replaceAll("_", "");
            int sign = +1;
            char first = value.charAt(0);
            if (first == '-') {
                sign = -1;
                value = value.substring(1);
            } else if (first == '+') {
                value = value.substring(1);
            }
            int base = 10;
            if ("0".equals(value)) {
                return new Integer(0);
            } else if (value.startsWith("0b")) {
                value = value.substring(2);
                base = 2;
            } else if (value.startsWith("0x")) {
                value = value.substring(2);
                base = 16;
            } else if (value.startsWith("0")) {
                value = value.substring(1);
                base = 8;
            } else if (value.indexOf(':') != -1) {
                String[] digits = value.split(":");
                int bes = 1;
                int val = 0;
                for (int i = 0, j = digits.length; i < j; i++) {
                    val += (Long.parseLong(digits[(j - i) - 1]) * bes);
                    bes *= 60;
                }
                return createNumber(sign, String.valueOf(val), 10);
            } else {
                return createNumber(sign, value, 10);
            }
            return createNumber(sign, value, base);
        }
    }

    private Number createNumber(int sign, String number, int radix) {
        Number result;
        if (sign < 0) {
            number = "-" + number;
        }
        try {
            result = Integer.valueOf(number, radix);
        } catch (NumberFormatException e) {
            try {
                result = Long.valueOf(number, radix);
            } catch (NumberFormatException e1) {
                result = new BigInteger(number, radix);
            }
        }
        return result;
    }

    private class ConstructYamlFloat extends AbstractConstruct {
        public Object construct(YamlNode node) {
            String value = constructScalar((ScalarNode) node).toString().replaceAll("_", "");
            int sign = +1;
            char first = value.charAt(0);
            if (first == '-') {
                sign = -1;
                value = value.substring(1);
            } else if (first == '+') {
                value = value.substring(1);
            }
            String valLower = value.toLowerCase();
            if (".inf".equals(valLower)) {
                return new Double(sign == -1 ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY);
            } else if (".nan".equals(valLower)) {
                return new Double(Double.NaN);
            } else if (value.indexOf(':') != -1) {
                String[] digits = value.split(":");
                int bes = 1;
                double val = 0.0;
                for (int i = 0, j = digits.length; i < j; i++) {
                    val += (Double.parseDouble(digits[(j - i) - 1]) * bes);
                    bes *= 60;
                }
                return new Double(sign * val);
            } else {
                Double d = Double.valueOf(value);
                return new Double(d.doubleValue() * sign);
            }
        }
    }

    private class ConstructYamlBinary extends AbstractConstruct {
        public Object construct(YamlNode node) {
            byte[] decoded = Base64Coder.decode(constructScalar((ScalarNode) node).toString()
                    .toCharArray());
            return decoded;
        }
    }

    private final static Pattern TIMESTAMP_REGEXP = Pattern
            .compile("^([0-9][0-9][0-9][0-9])-([0-9][0-9]?)-([0-9][0-9]?)(?:(?:[Tt]|[ \t]+)([0-9][0-9]?):([0-9][0-9]):([0-9][0-9])(?:\\.([0-9]*))?(?:[ \t]*(?:Z|([-+][0-9][0-9]?)(?::([0-9][0-9])?)?))?)?$");
    private final static Pattern YMD_REGEXP = Pattern
            .compile("^([0-9][0-9][0-9][0-9])-([0-9][0-9]?)-([0-9][0-9]?)$");

    protected class ConstructYamlTimestamp extends AbstractConstruct {
        private Calendar calendar;

        public Calendar getCalendar() {
            return calendar;
        }

        public Object construct(YamlNode node) {
            ScalarNode scalar = (ScalarNode) node;
            String nodeValue = scalar.getValue();
            Matcher match = YMD_REGEXP.matcher(nodeValue);
            if (match.matches()) {
                String year_s = match.group(1);
                String month_s = match.group(2);
                String day_s = match.group(3);
                calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                calendar.clear();
                calendar.set(Calendar.YEAR, Integer.parseInt(year_s));
                // Java's months are zero-based...
                calendar.set(Calendar.MONTH, Integer.parseInt(month_s) - 1); // x
                calendar.set(Calendar.DAY_OF_MONTH, Integer.parseInt(day_s));
                return calendar.getTime();
            } else {
                match = TIMESTAMP_REGEXP.matcher(nodeValue);
                if (!match.matches()) {
                    throw new YAMLException("Unexpected timestamp: " + nodeValue);
                }
                String year_s = match.group(1);
                String month_s = match.group(2);
                String day_s = match.group(3);
                String hour_s = match.group(4);
                String min_s = match.group(5);
                String sec_s = match.group(6);
                String fract_s = match.group(7);
                String timezoneh_s = match.group(8);
                String timezonem_s = match.group(9);

                int usec = 0;
                if (fract_s != null) {
                    usec = Integer.parseInt(fract_s);
                    if (usec != 0) {
                        while (10 * usec < 1000) {
                            usec *= 10;
                        }
                    }
                }
                TimeZone timeZone;
                if (timezoneh_s != null) {
                    String time = timezonem_s != null ? ":" + timezonem_s : "00";
                    timeZone = TimeZone.getTimeZone("GMT" + timezoneh_s + time);
                } else {
                    // no time zone provided
                    timeZone = TimeZone.getTimeZone("UTC");
                }
                calendar = Calendar.getInstance(timeZone);
                calendar.set(Calendar.YEAR, Integer.parseInt(year_s));
                // Java's months are zero-based...
                calendar.set(Calendar.MONTH, Integer.parseInt(month_s) - 1);
                calendar.set(Calendar.DAY_OF_MONTH, Integer.parseInt(day_s));
                calendar.set(Calendar.HOUR_OF_DAY, Integer.parseInt(hour_s));
                calendar.set(Calendar.MINUTE, Integer.parseInt(min_s));
                calendar.set(Calendar.SECOND, Integer.parseInt(sec_s));
                calendar.set(Calendar.MILLISECOND, usec);
                return calendar.getTime();
            }
        }
    }

    private class ConstructYamlOmap extends AbstractConstruct {
        public Object construct(YamlNode node) {
            // Note: we do not check for duplicate keys, because it's too
            // CPU-expensive.
            Map<Object, Object> omap = new LinkedHashMap<Object, Object>();
            if (!(node instanceof SequenceNode)) {
                throw new ConstructorException("while constructing an ordered map", node
                        .getStartMark(), "expected a sequence, but found " + node.getNodeId(), node
                        .getStartMark());
            }
            SequenceNode snode = (SequenceNode) node;
            for (YamlNode subnode : snode.getValue()) {
                if (!(subnode instanceof MappingNode)) {
                    throw new ConstructorException("while constructing an ordered map", node
                            .getStartMark(), "expected a mapping of length 1, but found "
                            + subnode.getNodeId(), subnode.getStartMark());
                }
                MappingNode mnode = (MappingNode) subnode;
                if (mnode.getValue().size() != 1) {
                    throw new ConstructorException("while constructing an ordered map", node
                            .getStartMark(), "expected a single mapping item, but found "
                            + mnode.getValue().size() + " items", mnode.getStartMark());
                }
                YamlNode keyNode = mnode.getValue().get(0).getKeyNode();
                YamlNode valueNode = mnode.getValue().get(0).getValueNode();
                Object key = constructObject(keyNode);
                Object value = constructObject(valueNode);
                omap.put(key, value);
            }
            return omap;
        }
    }

    // Note: the same code as `construct_yaml_omap`.

    private class ConstructYamlPairs extends AbstractConstruct {
        public Object construct(YamlNode node) {
            // Note: we do not check for duplicate keys, because it's too
            // CPU-expensive.
            if (!(node instanceof SequenceNode)) {
                throw new ConstructorException("while constructing pairs", node.getStartMark(),
                        "expected a sequence, but found " + node.getNodeId(), node.getStartMark());
            }
            SequenceNode snode = (SequenceNode) node;
            List<Object[]> pairs = new ArrayList<Object[]>(snode.getValue().size());
            for (YamlNode subnode : snode.getValue()) {
                if (!(subnode instanceof MappingNode)) {
                    throw new ConstructorException("while constructingpairs", node.getStartMark(),
                            "expected a mapping of length 1, but found " + subnode.getNodeId(),
                            subnode.getStartMark());
                }
                MappingNode mnode = (MappingNode) subnode;
                if (mnode.getValue().size() != 1) {
                    throw new ConstructorException("while constructing pairs", node.getStartMark(),
                            "expected a single mapping item, but found " + mnode.getValue().size()
                                    + " items", mnode.getStartMark());
                }
                YamlNode keyNode = mnode.getValue().get(0).getKeyNode();
                YamlNode valueNode = mnode.getValue().get(0).getValueNode();
                Object key = constructObject(keyNode);
                Object value = constructObject(valueNode);
                pairs.add(new Object[]{key, value});
            }
            return pairs;
        }
    }

    private class ConstructYamlSet implements Construct {
        public Object construct(YamlNode node) {
            if (node.isTwoStepsConstruction()) {
                return createDefaultSet();
            } else {
                return constructSet((MappingNode) node);
            }
        }

        @SuppressWarnings("unchecked")
        public void construct2ndStep(YamlNode node, Object object) {
            if (node.isTwoStepsConstruction()) {
                constructSet2ndStep((MappingNode) node, (Set<Object>) object);
            } else {
                throw new YAMLException("Unexpected recursive set structure. Node: " + node);
            }
        }
    }

    private class ConstructYamlStr extends AbstractConstruct {
        public Object construct(YamlNode node) {
            return (String) constructScalar((ScalarNode) node);
        }
    }

    private class ConstructYamlSeq implements Construct {
        public Object construct(YamlNode node) {
            SequenceNode seqNode = (SequenceNode) node;
            if (node.isTwoStepsConstruction()) {
                return createDefaultList((seqNode.getValue()).size());
            } else {
                return constructSequence(seqNode);
            }
        }

        @SuppressWarnings("unchecked")
        public void construct2ndStep(YamlNode node, Object data) {
            if (node.isTwoStepsConstruction()) {
                constructSequenceStep2((SequenceNode) node, (List<Object>) data);
            } else {
                throw new YAMLException("Unexpected recursive sequence structure. Node: " + node);
            }
        }
    }

    private class ConstructYamlMap implements Construct {
        public Object construct(YamlNode node) {
            if (node.isTwoStepsConstruction()) {
                return createDefaultMap();
            } else {
                return constructMapping((MappingNode) node);
            }
        }

        @SuppressWarnings("unchecked")
        public void construct2ndStep(YamlNode node, Object object) {
            if (node.isTwoStepsConstruction()) {
                constructMapping2ndStep((MappingNode) node, (Map<Object, Object>) object);
            } else {
                throw new YAMLException("Unexpected recursive mapping structure. Node: " + node);
            }
        }
    }

    private static final class ConstructUndefined extends AbstractConstruct {
        public Object construct(YamlNode node) {
            throw new ConstructorException(null, null,
                    "could not determine a constructor for the tag " + node.getTag(), node
                            .getStartMark());
        }
    }
}
