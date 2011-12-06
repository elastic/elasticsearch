/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.jmx;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.Classes;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.logging.ESLogger;

import javax.management.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.collect.MapBuilder.newMapBuilder;

/**
 *
 */
public class ResourceDMBean implements DynamicMBean {
    private static final Class<?>[] primitives = {int.class, byte.class, short.class, long.class,
            float.class, double.class, boolean.class, char.class};

    private final ESLogger logger;

    private final Object obj;

    private final String objectName;

    private final String groupName;

    private final String fullObjectName;

    private final String description;

    private final MBeanAttributeInfo[] attributesInfo;

    private final MBeanOperationInfo[] operationsInfo;

    private final MBeanInfo mBeanInfo;

    private final ImmutableMap<String, AttributeEntry> attributes;

    private final ImmutableList<MBeanOperationInfo> operations;

    public ResourceDMBean(Object instance, ESLogger logger) {
        Preconditions.checkNotNull(instance, "Cannot make an MBean wrapper for null instance");
        this.obj = instance;
        this.logger = logger;

        MapBuilder<String, AttributeEntry> attributesBuilder = newMapBuilder();
        List<MBeanOperationInfo> operationsBuilder = new ArrayList<MBeanOperationInfo>();

        MBean mBean = obj.getClass().getAnnotation(MBean.class);

        this.groupName = findGroupName();

        if (mBean != null && Strings.hasLength(mBean.objectName())) {
            objectName = mBean.objectName();
        } else {
            if (Strings.hasLength(groupName)) {
                // we have something in the group object name, don't put anything in the object name
                objectName = "";
            } else {
                objectName = obj.getClass().getSimpleName();
            }
        }

        StringBuilder sb = new StringBuilder(groupName);
        if (Strings.hasLength(groupName) && Strings.hasLength(objectName)) {
            sb.append(",");
        }
        sb.append(objectName);
        this.fullObjectName = sb.toString();

        this.description = findDescription();
        findFields(attributesBuilder);
        findMethods(attributesBuilder, operationsBuilder);

        this.attributes = attributesBuilder.immutableMap();
        this.operations = ImmutableList.copyOf(operationsBuilder);

        attributesInfo = new MBeanAttributeInfo[attributes.size()];
        int i = 0;

        MBeanAttributeInfo info;
        for (AttributeEntry entry : attributes.values()) {
            info = entry.getInfo();
            attributesInfo[i++] = info;
            if (logger.isInfoEnabled()) {
                logger.trace("Attribute " + info.getName() + "[r=" + info.isReadable() + ",w="
                        + info.isWritable() + ",is=" + info.isIs() + ",type=" + info.getType() + "]");
            }
        }

        operationsInfo = new MBeanOperationInfo[operations.size()];
        operations.toArray(operationsInfo);

        if (logger.isTraceEnabled()) {
            if (operations.size() > 0)
                logger.trace("Operations are:");
            for (MBeanOperationInfo op : operationsInfo) {
                logger.trace("Operation " + op.getReturnType() + " " + op.getName());
            }
        }

        this.mBeanInfo = new MBeanInfo(getObject().getClass().getCanonicalName(), description, attributesInfo, null, operationsInfo, null);
    }

    public MBeanInfo getMBeanInfo() {
        return mBeanInfo;
    }

    public synchronized Object getAttribute(String name) throws AttributeNotFoundException {
        if (name == null || name.length() == 0)
            throw new NullPointerException("Invalid attribute requested " + name);

        Attribute attr = getNamedAttribute(name);
        if (attr == null) {
            throw new AttributeNotFoundException("Unknown attribute '" + name
                    + "'. Known attributes names are: " + attributes.keySet());
        }
        return attr.getValue();
    }

    public synchronized void setAttribute(Attribute attribute) {
        if (attribute == null || attribute.getName() == null)
            throw new NullPointerException("Invalid attribute requested " + attribute);

        setNamedAttribute(attribute);
    }

    public synchronized AttributeList getAttributes(String[] names) {
        AttributeList al = new AttributeList();
        for (String name : names) {
            Attribute attr = getNamedAttribute(name);
            if (attr != null) {
                al.add(attr);
            } else {
                logger.warn("Did not find attribute " + name);
            }
        }
        return al;
    }

    public synchronized AttributeList setAttributes(AttributeList list) {
        AttributeList results = new AttributeList();
        for (Object aList : list) {
            Attribute attr = (Attribute) aList;

            if (setNamedAttribute(attr)) {
                results.add(attr);
            } else {
                if (logger.isWarnEnabled()) {
                    logger.warn("Failed to update attribute name " + attr.getName() + " with value "
                            + attr.getValue());
                }
            }
        }
        return results;
    }

    public Object invoke(String name, Object[] args, String[] sig) throws MBeanException,
            ReflectionException {
        if (logger.isDebugEnabled()) {
            logger.debug("Invoke method called on " + name);
        }

        MBeanOperationInfo opInfo = null;
        for (MBeanOperationInfo op : operationsInfo) {
            if (op.getName().equals(name)) {
                opInfo = op;
                break;
            }
        }

        if (opInfo == null) {
            final String msg = "Operation " + name + " not in ModelMBeanInfo";
            throw new MBeanException(new ServiceNotFoundException(msg), msg);
        }

        try {
            Class<?>[] classes = new Class[sig.length];
            for (int i = 0; i < classes.length; i++) {
                classes[i] = getClassForName(sig[i]);
            }
            Method method = getObject().getClass().getMethod(name, classes);
            return method.invoke(getObject(), args);
        } catch (Exception e) {
            throw new MBeanException(e);
        }
    }

    Object getObject() {
        return obj;
    }

    private Class<?> getClassForName(String name) throws ClassNotFoundException {
        try {
            return Classes.getDefaultClassLoader().loadClass(name);
        } catch (ClassNotFoundException cnfe) {
            // Could be a primitive - let's check
            for (Class<?> primitive : primitives) {
                if (name.equals(primitive.getName())) {
                    return primitive;
                }
            }
        }
        throw new ClassNotFoundException("Class " + name + " cannot be found");
    }

    private String findGroupName() {
        Class objClass = getObject().getClass();
        while (objClass != Object.class) {
            Method[] methods = objClass.getDeclaredMethods();
            for (Method method : methods) {
                if (method.isAnnotationPresent(ManagedGroupName.class)) {
                    try {
                        method.setAccessible(true);
                        return (String) method.invoke(getObject());
                    } catch (Exception e) {
                        logger.warn("Failed to get group name for [" + getObject() + "]", e);
                    }
                }
            }
            objClass = objClass.getSuperclass();
        }
        return "";
    }

    private String findDescription() {
        MBean mbean = getObject().getClass().getAnnotation(MBean.class);
        if (mbean != null && mbean.description() != null && mbean.description().trim().length() > 0) {
            return mbean.description();
        }
        return "";
    }

    private void findMethods(MapBuilder<String, AttributeEntry> attributesBuilder, List<MBeanOperationInfo> ops) {
        // find all methods but don't include methods from Object class
        List<Method> methods = new ArrayList<Method>(Arrays.asList(getObject().getClass().getMethods()));
        List<Method> objectMethods = new ArrayList<Method>(Arrays.asList(Object.class.getMethods()));
        methods.removeAll(objectMethods);

        for (Method method : methods) {
            // does method have @ManagedAttribute annotation?
            ManagedAttribute attr = method.getAnnotation(ManagedAttribute.class);
            if (attr != null) {
                String methodName = method.getName();
                if (!methodName.startsWith("get") && !methodName.startsWith("set") && !methodName.startsWith("is")) {
                    if (logger.isWarnEnabled())
                        logger.warn("method name " + methodName
                                + " doesn't start with \"get\", \"set\", or \"is\""
                                + ", but is annotated with @ManagedAttribute: will be ignored");
                } else {
                    MBeanAttributeInfo info;
                    String attributeName = null;
                    boolean writeAttribute = false;
                    if (isSetMethod(method)) { // setter
                        attributeName = methodName.substring(3);
                        info = new MBeanAttributeInfo(attributeName, method.getParameterTypes()[0]
                                .getCanonicalName(), attr.description(), true, true, false);
                        writeAttribute = true;
                    } else { // getter
                        if (method.getParameterTypes().length == 0
                                && method.getReturnType() != java.lang.Void.TYPE) {
                            boolean hasSetter = attributesBuilder.containsKey(attributeName);
                            // we found is method
                            if (methodName.startsWith("is")) {
                                attributeName = methodName.substring(2);
                                info = new MBeanAttributeInfo(attributeName, method.getReturnType()
                                        .getCanonicalName(), attr.description(), true, hasSetter, true);
                            } else {
                                // this has to be get
                                attributeName = methodName.substring(3);
                                info = new MBeanAttributeInfo(attributeName, method.getReturnType()
                                        .getCanonicalName(), attr.description(), true, hasSetter, false);
                            }
                        } else {
                            if (logger.isWarnEnabled()) {
                                logger.warn("Method " + method.getName()
                                        + " must have a valid return type and zero parameters");
                            }
                            continue;
                        }
                    }

                    AttributeEntry ae = attributesBuilder.get(attributeName);
                    // is it a read method?
                    if (!writeAttribute) {
                        // we already have annotated field as read
                        if (ae instanceof FieldAttributeEntry && ae.getInfo().isReadable()) {
                            logger.warn("not adding annotated method " + method
                                    + " since we already have read attribute");
                        }
                        // we already have annotated set method
                        else if (ae instanceof MethodAttributeEntry) {
                            MethodAttributeEntry mae = (MethodAttributeEntry) ae;
                            if (mae.hasSetMethod()) {
                                attributesBuilder.put(attributeName, new MethodAttributeEntry(mae.getInfo(), mae
                                        .getSetMethod(), method));
                            }
                        } // we don't have such entry
                        else {
                            attributesBuilder.put(attributeName, new MethodAttributeEntry(info, null, method));
                        }
                    }// is it a set method?
                    else {
                        if (ae instanceof FieldAttributeEntry) {
                            // we already have annotated field as write
                            if (ae.getInfo().isWritable()) {
                                logger.warn("Not adding annotated method " + methodName
                                        + " since we already have writable attribute");
                            } else {
                                // we already have annotated field as read
                                // lets make the field writable
                                Field f = ((FieldAttributeEntry) ae).getField();
                                MBeanAttributeInfo i = new MBeanAttributeInfo(ae.getInfo().getName(),
                                        f.getType().getCanonicalName(), attr.description(), true,
                                        !Modifier.isFinal(f.getModifiers()), false);
                                attributesBuilder.put(attributeName, new FieldAttributeEntry(i, f));
                            }
                        }
                        // we already have annotated getOrIs method
                        else if (ae instanceof MethodAttributeEntry) {
                            MethodAttributeEntry mae = (MethodAttributeEntry) ae;
                            if (mae.hasIsOrGetMethod()) {
                                attributesBuilder.put(attributeName, new MethodAttributeEntry(info, method, mae
                                        .getIsOrGetMethod()));
                            }
                        } // we don't have such entry
                        else {
                            attributesBuilder.put(attributeName, new MethodAttributeEntry(info, method, null));
                        }
                    }
                }
            } else if (method.isAnnotationPresent(ManagedOperation.class)) {
                ManagedOperation op = method.getAnnotation(ManagedOperation.class);
                String attName = method.getName();
                if (isSetMethod(method) || isGetMethod(method)) {
                    attName = attName.substring(3);
                } else if (isIsMethod(method)) {
                    attName = attName.substring(2);
                }
                // expose unless we already exposed matching attribute field
                boolean isAlreadyExposed = attributesBuilder.containsKey(attName);
                if (!isAlreadyExposed) {
                    ops.add(new MBeanOperationInfo(op != null ? op.description() : "", method));
                }
            }
        }
    }

    private boolean isSetMethod(Method method) {
        return (method.getName().startsWith("set") && method.getParameterTypes().length == 1 && method.getReturnType() == java.lang.Void.TYPE);
    }

    private boolean isGetMethod(Method method) {
        return (method.getParameterTypes().length == 0 && method.getReturnType() != java.lang.Void.TYPE && method.getName().startsWith("get"));
    }

    private boolean isIsMethod(Method method) {
        return (method.getParameterTypes().length == 0 && (method.getReturnType() == boolean.class || method.getReturnType() == Boolean.class) && method.getName().startsWith("is"));
    }

    private void findFields(MapBuilder<String, AttributeEntry> attributesBuilder) {
        // traverse class hierarchy and find all annotated fields
        for (Class<?> clazz = getObject().getClass(); clazz != null; clazz = clazz.getSuperclass()) {
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                ManagedAttribute attr = field.getAnnotation(ManagedAttribute.class);
                if (attr != null) {
                    String fieldName = renameToJavaCodingConvention(field.getName());
                    MBeanAttributeInfo info = new MBeanAttributeInfo(fieldName, field.getType().getCanonicalName(),
                            attr.description(), true, !Modifier.isFinal(field.getModifiers()) && attr.writable(), false);
                    attributesBuilder.put(fieldName, new FieldAttributeEntry(info, field));
                }
            }
        }
    }

    private Attribute getNamedAttribute(String name) {
        Attribute result = null;
        AttributeEntry entry = attributes.get(name);
        if (entry != null) {
            MBeanAttributeInfo i = entry.getInfo();
            try {
                result = new Attribute(name, entry.invoke(null));
                if (logger.isDebugEnabled())
                    logger.debug("Attribute " + name + " has r=" + i.isReadable() + ",w="
                            + i.isWritable() + ",is=" + i.isIs() + " and value "
                            + result.getValue());
            } catch (Exception e) {
                logger.debug("Exception while reading value of attribute " + name, e);
            }
        } else {
            logger.warn("Did not find queried attribute with name " + name);
        }
        return result;
    }

    private boolean setNamedAttribute(Attribute attribute) {
        boolean result = false;
        if (logger.isDebugEnabled())
            logger.debug("Invoking set on attribute " + attribute.getName() + " with value " + attribute.getValue());

        AttributeEntry entry = attributes.get(attribute.getName());
        if (entry != null) {
            try {
                entry.invoke(attribute);
                result = true;
            } catch (Exception e) {
                logger.warn("Exception while writing value for attribute " + attribute.getName(), e);
            }
        } else {
            logger.warn("Could not invoke set on attribute " + attribute.getName() + " with value "
                    + attribute.getValue());
        }
        return result;
    }

    private String renameToJavaCodingConvention(String fieldName) {
        if (fieldName.contains("_")) {
            Pattern p = Pattern.compile("_.");
            Matcher m = p.matcher(fieldName);
            StringBuffer sb = new StringBuffer();
            while (m.find()) {
                m.appendReplacement(sb, fieldName.substring(m.end() - 1, m.end()).toUpperCase());
            }
            m.appendTail(sb);
            char first = sb.charAt(0);
            if (Character.isLowerCase(first)) {
                sb.setCharAt(0, Character.toUpperCase(first));
            }
            return sb.toString();
        } else {
            if (Character.isLowerCase(fieldName.charAt(0))) {
                return fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
            } else {
                return fieldName;
            }
        }
    }

    private class MethodAttributeEntry implements AttributeEntry {

        final MBeanAttributeInfo info;

        final Method isOrGetmethod;

        final Method setMethod;

        public MethodAttributeEntry(final MBeanAttributeInfo info, final Method setMethod,
                                    final Method isOrGetMethod) {
            super();
            this.info = info;
            this.setMethod = setMethod;
            this.isOrGetmethod = isOrGetMethod;
        }

        public Object invoke(Attribute a) throws Exception {
            if (a == null && isOrGetmethod != null)
                return isOrGetmethod.invoke(getObject());
            else if (a != null && setMethod != null)
                return setMethod.invoke(getObject(), a.getValue());
            else
                return null;
        }

        public MBeanAttributeInfo getInfo() {
            return info;
        }

        public boolean hasIsOrGetMethod() {
            return isOrGetmethod != null;
        }

        public boolean hasSetMethod() {
            return setMethod != null;
        }

        public Method getIsOrGetMethod() {
            return isOrGetmethod;
        }

        public Method getSetMethod() {
            return setMethod;
        }
    }

    private class FieldAttributeEntry implements AttributeEntry {

        private final MBeanAttributeInfo info;

        private final Field field;

        public FieldAttributeEntry(final MBeanAttributeInfo info, final Field field) {
            super();
            this.info = info;
            this.field = field;
            if (!field.isAccessible()) {
                field.setAccessible(true);
            }
        }

        public Field getField() {
            return field;
        }

        public Object invoke(Attribute a) throws Exception {
            if (a == null) {
                return field.get(getObject());
            } else {
                field.set(getObject(), a.getValue());
                return null;
            }
        }

        public MBeanAttributeInfo getInfo() {
            return info;
        }
    }

    private interface AttributeEntry {
        public Object invoke(Attribute a) throws Exception;

        public MBeanAttributeInfo getInfo();
    }

    public boolean isManagedResource() {
        return !attributes.isEmpty() || !operations.isEmpty();
    }

    public String getFullObjectName() {
        return this.fullObjectName;
    }

    public String getObjectName() {
        return this.objectName;
    }

    public String getGroupName() {
        return this.groupName;
    }
}