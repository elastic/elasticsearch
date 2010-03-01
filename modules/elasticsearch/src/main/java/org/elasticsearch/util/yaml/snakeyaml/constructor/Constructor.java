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

import org.elasticsearch.util.yaml.snakeyaml.TypeDescription;
import org.elasticsearch.util.yaml.snakeyaml.error.YAMLException;
import org.elasticsearch.util.yaml.snakeyaml.introspector.FieldProperty;
import org.elasticsearch.util.yaml.snakeyaml.introspector.MethodProperty;
import org.elasticsearch.util.yaml.snakeyaml.introspector.Property;
import org.elasticsearch.util.yaml.snakeyaml.nodes.*;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

/**
 * Construct a custom Java instance.
 */
public class Constructor extends SafeConstructor {
    private final Map<Tag, Class<? extends Object>> typeTags;
    private final Map<Class<? extends Object>, TypeDescription> typeDefinitions;

    public Constructor() {
        this(Object.class);
    }

    /**
     * Create Constructor for the specified class as the root.
     *
     * @param theRoot - the class (usually JavaBean) to be constructed
     */
    public Constructor(Class<? extends Object> theRoot) {
        if (theRoot == null) {
            throw new NullPointerException("Root type must be provided.");
        }
        this.yamlConstructors.put(null, new ConstructYamlObject());
        if (!Object.class.equals(theRoot)) {
            rootTag = new Tag(theRoot);
        }
        typeTags = new HashMap<Tag, Class<? extends Object>>();
        typeDefinitions = new HashMap<Class<? extends Object>, TypeDescription>();
        yamlClassConstructors.put(NodeId.scalar, new ConstructScalar());
        yamlClassConstructors.put(NodeId.mapping, new ConstructMapping());
        yamlClassConstructors.put(NodeId.sequence, new ConstructSequence());
    }

    /**
     * Create Constructor for a class which does not have to be in the classpath
     * or for a definition from a Spring ApplicationContext.
     *
     * @param theRoot fully qualified class name of the root class (usually
     *                JavaBean)
     * @throws ClassNotFoundException
     */
    public Constructor(String theRoot) throws ClassNotFoundException {
        this(Class.forName(check(theRoot)));
    }

    private static final String check(String s) {
        if (s == null) {
            throw new NullPointerException("Root type must be provided.");
        }
        if (s.trim().length() == 0) {
            throw new YAMLException("Root type must be provided.");
        }
        return s;
    }

    /**
     * Make YAML aware how to parse a custom Class. If there is no root Class
     * assigned in constructor then the 'root' property of this definition is
     * respected.
     *
     * @param definition to be added to the Constructor
     * @return the previous value associated with <tt>definition</tt>, or
     *         <tt>null</tt> if there was no mapping for <tt>definition</tt>.
     */
    public TypeDescription addTypeDescription(TypeDescription definition) {
        if (definition == null) {
            throw new NullPointerException("TypeDescription is required.");
        }
        if (rootTag == null && definition.isRoot()) {
            rootTag = new Tag(definition.getType());
        }
        Tag tag = definition.getTag();
        typeTags.put(tag, definition.getType());
        return typeDefinitions.put(definition.getType(), definition);
    }

    /**
     * Construct mapping instance (Map, JavaBean) when the runtime class is
     * known.
     */
    private class ConstructMapping implements Construct {
        private final Map<Class<? extends Object>, Map<String, Property>> propertiesCache = new HashMap<Class<? extends Object>, Map<String, Property>>();

        /**
         * Construct JavaBean. If type safe collections are used please look at
         * <code>TypeDescription</code>.
         *
         * @param node node where the keys are property names (they can only be
         *             <code>String</code>s) and values are objects to be created
         * @return constructed JavaBean
         */
        public Object construct(Node node) {
            MappingNode mnode = (MappingNode) node;
            if (Properties.class.isAssignableFrom(node.getType())) {
                Properties properties = new Properties();
                if (!node.isTwoStepsConstruction()) {
                    constructMapping2ndStep(mnode, (Map<Object, Object>) properties);
                } else {
                    throw new YAMLException("Properties must not be recursive.");
                }
                return properties;
            } else if (SortedMap.class.isAssignableFrom(node.getType())) {
                SortedMap<Object, Object> map = new TreeMap<Object, Object>();
                if (!node.isTwoStepsConstruction()) {
                    constructMapping2ndStep(mnode, map);
                }
                return map;
            } else if (Map.class.isAssignableFrom(node.getType())) {
                if (node.isTwoStepsConstruction()) {
                    return createDefaultMap();
                } else {
                    return constructMapping(mnode);
                }
            } else if (SortedSet.class.isAssignableFrom(node.getType())) {
                SortedSet<Object> set = new TreeSet<Object>();
                if (!node.isTwoStepsConstruction()) {
                    constructSet2ndStep(mnode, set);
                }
                return set;
            } else if (Set.class.isAssignableFrom(node.getType())) {
                if (node.isTwoStepsConstruction()) {
                    return createDefaultSet();
                } else {
                    return constructSet(mnode);
                }
            } else {
                if (node.isTwoStepsConstruction()) {
                    return createEmptyJavaBean(mnode);
                } else {
                    return constructJavaBean2ndStep(mnode, createEmptyJavaBean(mnode));
                }
            }
        }

        @SuppressWarnings("unchecked")
        public void construct2ndStep(Node node, Object object) {
            if (Map.class.isAssignableFrom(node.getType())) {
                constructMapping2ndStep((MappingNode) node, (Map<Object, Object>) object);
            } else if (Set.class.isAssignableFrom(node.getType())) {
                constructSet2ndStep((MappingNode) node, (Set<Object>) object);
            } else {
                constructJavaBean2ndStep((MappingNode) node, object);
            }
        }

        private Object createEmptyJavaBean(MappingNode node) {
            try {
                Class<? extends Object> type = node.getType();
                if (Modifier.isAbstract(type.getModifiers())) {
                    node.setType(getClassForNode(node));
                }
                /**
                 * Using only default constructor. Everything else will be
                 * initialized on 2nd step. If we do here some partial
                 * initialization, how do we then track what need to be done on
                 * 2nd step? I think it is better to get only object here (to
                 * have it as reference for recursion) and do all other thing on
                 * 2nd step.
                 */
                return node.getType().newInstance();
            } catch (InstantiationException e) {
                throw new YAMLException(e);
            } catch (IllegalAccessException e) {
                throw new YAMLException(e);
            }
        }

        @SuppressWarnings("unchecked")
        private Object constructJavaBean2ndStep(MappingNode node, Object object) {
            Class<? extends Object> beanType = node.getType();
            List<NodeTuple> nodeValue = (List<NodeTuple>) node.getValue();
            for (NodeTuple tuple : nodeValue) {
                ScalarNode keyNode;
                if (tuple.getKeyNode() instanceof ScalarNode) {
                    // key must be scalar
                    keyNode = (ScalarNode) tuple.getKeyNode();
                } else {
                    throw new YAMLException("Keys must be scalars but found: " + tuple.getKeyNode());
                }
                Node valueNode = tuple.getValueNode();
                // keys can only be Strings
                keyNode.setType(String.class);
                String key = (String) constructObject(keyNode);
                boolean isArray = false;
                try {
                    Property property = getProperty(beanType, key);
                    valueNode.setType(property.getType());
                    if (property.getType().isArray()) {
                        isArray = true;
                    }
                    TypeDescription memberDescription = typeDefinitions.get(beanType);
                    boolean typeDetected = false;
                    if (memberDescription != null) {
                        switch (valueNode.getNodeId()) {
                            case sequence:
                                SequenceNode snode = (SequenceNode) valueNode;
                                Class<? extends Object> memberType = memberDescription
                                        .getListPropertyType(key);
                                if (memberType != null) {
                                    snode.setListType(memberType);
                                    typeDetected = true;
                                } else if (property.getType().isArray()) {
                                    isArray = true;
                                    snode.setListType(property.getType().getComponentType());
                                    typeDetected = true;
                                }
                                break;
                            case mapping:
                                MappingNode mnode = (MappingNode) valueNode;
                                Class<? extends Object> keyType = memberDescription.getMapKeyType(key);
                                if (keyType != null) {
                                    mnode.setKeyType(keyType);
                                    mnode.setValueType(memberDescription.getMapValueType(key));
                                    typeDetected = true;
                                }
                                break;
                        }
                    }
                    if (!typeDetected && valueNode.getNodeId() != NodeId.scalar) {
                        // only if there is no explicit TypeDescription
                        Type[] arguments = property.getActualTypeArguments();
                        if (arguments != null) {
                            // type safe (generic) collection may contain the
                            // proper class
                            if (valueNode.getNodeId() == NodeId.sequence) {
                                Class t = (Class) arguments[0];
                                SequenceNode snode = (SequenceNode) valueNode;
                                snode.setListType(t);
                            } else if (valueNode.getTag().equals(Tag.SET)) {
                                Class t = (Class) arguments[0];
                                MappingNode mnode = (MappingNode) valueNode;
                                mnode.setKeyType(t);
                                mnode.setUseClassConstructor(true);
                            } else if (valueNode.getNodeId() == NodeId.mapping) {
                                Class ketType = (Class) arguments[0];
                                Class valueType = (Class) arguments[1];
                                MappingNode mnode = (MappingNode) valueNode;
                                mnode.setKeyType(ketType);
                                mnode.setValueType(valueType);
                                mnode.setUseClassConstructor(true);
                            }
                        }
                    }
                    Object value = constructObject(valueNode);
                    if (isArray && value instanceof List) {
                        List<Object> list = (List<Object>) value;
                        value = list.toArray(createArray(property.getType()));
                    }
                    property.set(object, value);
                } catch (Exception e) {
                    throw new YAMLException("Cannot create property=" + key + " for JavaBean="
                            + object + "; " + e.getMessage(), e);
                }
            }
            return object;
        }

        @SuppressWarnings("unchecked")
        private <T> T[] createArray(Class<T> type) {
            return (T[]) Array.newInstance(type.getComponentType(), 0);
        }

        private Property getProperty(Class<? extends Object> type, String name)
                throws IntrospectionException {
            // check cache
            Map<String, Property> properties = propertiesCache.get(type);
            if (properties == null) {
                properties = new HashMap<String, Property>();
                propertiesCache.put(type, properties);
                for (PropertyDescriptor property : Introspector.getBeanInfo(type)
                        .getPropertyDescriptors()) {
                    String methodName = property.getName();
                    if (property.getWriteMethod() != null) {
                        properties.put(methodName, new MethodProperty(property));
                    }
                }
                for (Field field : type.getFields()) {
                    int modifiers = field.getModifiers();
                    if (Modifier.isStatic(modifiers) || Modifier.isTransient(modifiers)) {
                        continue;
                    }
                    properties.put(field.getName(), new FieldProperty(field));
                }
            }
            // take from cache
            Property property = properties.get(name);
            if (property == null) {
                throw new YAMLException("Unable to find property '" + name + "' on class: "
                        + type.getName());
            } else {
                return property;
            }
        }
    }

    /**
     * Construct an instance when the runtime class is not known but a global
     * tag with a class name is defined. It delegates the construction to the
     * appropriate constructor based on the node kind (scalar, sequence,
     * mapping)
     */
    private class ConstructYamlObject implements Construct {

        @SuppressWarnings("unchecked")
        private Construct getConstructor(Node node) {
            Class cl = getClassForNode(node);
            node.setType(cl);
            // call the constructor as if the runtime class is defined
            Construct constructor = yamlClassConstructors.get(node.getNodeId());
            return constructor;
        }

        public Object construct(Node node) {
            Object result = null;
            try {
                result = getConstructor(node).construct(node);
            } catch (Exception e) {
                throw new ConstructorException(null, null, "Can't construct a java object for "
                        + node.getTag() + "; exception=" + e.getMessage(), node.getStartMark(), e);
            }
            return result;
        }

        public void construct2ndStep(Node node, Object object) {
            try {
                getConstructor(node).construct2ndStep(node, object);
            } catch (Exception e) {
                throw new ConstructorException(null, null,
                        "Can't construct a second step for a java object for " + node.getTag()
                                + "; exception=" + e.getMessage(), node.getStartMark(), e);
            }
        }
    }

    /**
     * Construct scalar instance when the runtime class is known. Recursive
     * structures are not supported.
     */
    protected class ConstructScalar extends AbstractConstruct {
        @SuppressWarnings("unchecked")
        public Object construct(Node nnode) {
            ScalarNode node = (ScalarNode) nnode;
            Class type = node.getType();
            Object result;
            if (type.isPrimitive() || type == String.class || Number.class.isAssignableFrom(type)
                    || type == Boolean.class || Date.class.isAssignableFrom(type)
                    || type == Character.class || type == BigInteger.class
                    || type == BigDecimal.class || Enum.class.isAssignableFrom(type)
                    || Tag.BINARY.equals(node.getTag()) || Calendar.class.isAssignableFrom(type)) {
                // standard classes created directly
                result = constructStandardJavaInstance(type, node);
            } else {
                // there must be only 1 constructor with 1 argument
                java.lang.reflect.Constructor[] javaConstructors = type.getConstructors();
                int oneArgCount = 0;
                java.lang.reflect.Constructor javaConstructor = null;
                for (java.lang.reflect.Constructor c : javaConstructors) {
                    if (c.getParameterTypes().length == 1) {
                        oneArgCount++;
                        javaConstructor = c;
                    }
                }
                Object argument;
                if (javaConstructor == null) {
                    throw new YAMLException("No single argument constructor found for " + type);
                } else if (oneArgCount == 1) {
                    argument = constructStandardJavaInstance(
                            javaConstructor.getParameterTypes()[0], node);
                } else {
                    // TODO it should be possible to use implicit types instead
                    // of forcing String. Resolver must be available here to
                    // obtain the implicit tag. Then we can set the tag and call
                    // callConstructor(node) to create the argument instance.
                    // On the other hand it may be safer to require a custom
                    // constructor to avoid guessing the argument class
                    argument = constructScalar(node);
                    try {
                        javaConstructor = type.getConstructor(String.class);
                    } catch (Exception e) {
                        throw new ConstructorException(null, null,
                                "Can't construct a java object for scalar " + node.getTag()
                                        + "; No String constructor found. Exception="
                                        + e.getMessage(), node.getStartMark(), e);
                    }
                }
                try {
                    result = javaConstructor.newInstance(argument);
                } catch (Exception e) {
                    throw new ConstructorException(null, null,
                            "Can't construct a java object for scalar " + node.getTag()
                                    + "; exception=" + e.getMessage(), node.getStartMark(), e);
                }
            }
            return result;
        }

        @SuppressWarnings("unchecked")
        private Object constructStandardJavaInstance(Class type, ScalarNode node) {
            Object result;
            if (type == String.class) {
                Construct stringConstructor = yamlConstructors.get(Tag.STR);
                result = stringConstructor.construct((ScalarNode) node);
            } else if (type == Boolean.class || type == Boolean.TYPE) {
                Construct boolConstructor = yamlConstructors.get(Tag.BOOL);
                result = boolConstructor.construct((ScalarNode) node);
            } else if (type == Character.class || type == Character.TYPE) {
                Construct charConstructor = yamlConstructors.get(Tag.STR);
                String ch = (String) charConstructor.construct((ScalarNode) node);
                if (ch.length() == 0) {
                    result = null;
                } else if (ch.length() != 1) {
                    throw new YAMLException("Invalid node Character: '" + ch + "'; length: "
                            + ch.length());
                } else {
                    result = new Character(ch.charAt(0));
                }
            } else if (Date.class.isAssignableFrom(type)) {
                Construct dateConstructor = yamlConstructors.get(Tag.TIMESTAMP);
                Date date = (Date) dateConstructor.construct((ScalarNode) node);
                if (type == Date.class) {
                    result = date;
                } else {
                    try {
                        java.lang.reflect.Constructor<?> constr = type.getConstructor(long.class);
                        result = constr.newInstance(date.getTime());
                    } catch (Exception e) {
                        throw new YAMLException("Cannot construct: '" + type + "'");
                    }
                }
            } else if (type == Float.class || type == Double.class || type == Float.TYPE
                    || type == Double.TYPE || type == BigDecimal.class) {
                if (type == BigDecimal.class) {
                    result = new BigDecimal(node.getValue());
                } else {
                    Construct doubleConstructor = yamlConstructors.get(Tag.FLOAT);
                    result = doubleConstructor.construct(node);
                    if (type == Float.class || type == Float.TYPE) {
                        result = new Float((Double) result);
                    }
                }
            } else if (type == Byte.class || type == Short.class || type == Integer.class
                    || type == Long.class || type == BigInteger.class || type == Byte.TYPE
                    || type == Short.TYPE || type == Integer.TYPE || type == Long.TYPE) {
                Construct intConstructor = yamlConstructors.get(Tag.INT);
                result = intConstructor.construct(node);
                if (type == Byte.class || type == Byte.TYPE) {
                    result = new Byte(result.toString());
                } else if (type == Short.class || type == Short.TYPE) {
                    result = new Short(result.toString());
                } else if (type == Integer.class || type == Integer.TYPE) {
                    result = new Integer(result.toString());
                } else if (type == Long.class || type == Long.TYPE) {
                    result = new Long(result.toString());
                } else {
                    // only BigInteger left
                    result = new BigInteger(result.toString());
                }
            } else if (Enum.class.isAssignableFrom(type)) {
                String enumValueName = node.getValue();
                try {
                    result = Enum.valueOf(type, enumValueName);
                } catch (Exception ex) {
                    throw new YAMLException("Unable to find enum value '" + enumValueName
                            + "' for enum class: " + type.getName());
                }
            } else if (Calendar.class.isAssignableFrom(type)) {
                ConstructYamlTimestamp contr = new ConstructYamlTimestamp();
                contr.construct(node);
                result = contr.getCalendar();
            } else {
                throw new YAMLException("Unsupported class: " + type);
            }
            return result;
        }
    }

    /**
     * Construct sequence (List, Array, or immutable object) when the runtime
     * class is known.
     */
    private class ConstructSequence implements Construct {
        @SuppressWarnings("unchecked")
        public Object construct(Node node) {
            SequenceNode snode = (SequenceNode) node;
            if (List.class.isAssignableFrom(node.getType()) || node.getType().isArray()) {
                if (node.isTwoStepsConstruction()) {
                    return createDefaultList(snode.getValue().size());
                } else {
                    return constructSequence(snode);
                }
            } else {
                // create immutable object
                List<java.lang.reflect.Constructor> possibleConstructors = new ArrayList<java.lang.reflect.Constructor>(
                        snode.getValue().size());
                for (java.lang.reflect.Constructor constructor : node.getType().getConstructors()) {
                    if (snode.getValue().size() == constructor.getParameterTypes().length) {
                        possibleConstructors.add(constructor);
                    }
                }
                if (possibleConstructors.isEmpty()) {
                    throw new YAMLException("No constructors with "
                            + String.valueOf(snode.getValue().size()) + " arguments found for "
                            + node.getType());
                }
                List<Object> argumentList;
                if (possibleConstructors.size() == 1) {
                    argumentList = new ArrayList<Object>(snode.getValue().size());
                    java.lang.reflect.Constructor c = possibleConstructors.get(0);
                    int index = 0;
                    for (Node argumentNode : snode.getValue()) {
                        Class type = c.getParameterTypes()[index];
                        // set runtime classes for arguments
                        argumentNode.setType(type);
                        Object argumentValue = constructObject(argumentNode);
                        argumentList.add(argumentValue);
                        index++;
                    }
                } else {
                    // use BaseConstructor
                    argumentList = (List<Object>) constructSequence(snode);
                }
                Class[] parameterTypes = new Class[argumentList.size()];
                int index = 0;
                for (Object parameter : argumentList) {
                    parameterTypes[index] = parameter.getClass();
                    index++;
                }
                java.lang.reflect.Constructor javaConstructor;
                try {
                    Class cl = node.getType();
                    javaConstructor = cl.getConstructor(parameterTypes);
                    Object[] initargs = argumentList.toArray();
                    return javaConstructor.newInstance(initargs);
                } catch (Exception e) {
                    throw new YAMLException(e);
                }
            }
        }

        @SuppressWarnings("unchecked")
        public void construct2ndStep(Node node, Object object) {
            SequenceNode snode = (SequenceNode) node;
            List<Object> list = (List<Object>) object;
            if (List.class.isAssignableFrom(node.getType())) {
                constructSequenceStep2(snode, list);
            } else {
                throw new YAMLException("Immutable objects cannot be recursive.");
            }
        }
    }

    protected Class<?> getClassForNode(Node node) {
        Class<? extends Object> classForTag = typeTags.get(node.getTag());
        if (classForTag == null) {
            String name = node.getTag().getClassName();
            Class<?> cl;
            try {
                cl = getClassForName(name);
            } catch (ClassNotFoundException e) {
                throw new YAMLException("Class not found: " + name);
            }
            typeTags.put(node.getTag(), cl);
            return cl;
        } else {
            return classForTag;
        }
    }

    protected Class<?> getClassForName(String name) throws ClassNotFoundException {
        return Class.forName(name);
    }
}
