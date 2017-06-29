/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.tree;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.util.Assert;
import org.elasticsearch.xpack.sql.util.ObjectUtils;

import java.util.Objects;
import java.util.Set;

import static java.lang.String.format;

public abstract class NodeUtils {

    public static class NodeInfo {
        public final Constructor<?> ctr;
        public final Map<String, Method> params;
        private final int childrenIndex;

        NodeInfo(Constructor<?> ctr, Map<String, Method> params, int childrenIndex) {
            this.ctr = ctr;
            this.params = params;
            this.childrenIndex = childrenIndex;
        }
    };

    private static final String TO_STRING_IGNORE_PROP = "location";
    private static final int TO_STRING_MAX_PROP = 10;

    private static final Map<Class<?>, NodeInfo> CACHE = new LinkedHashMap<>();
    
    // make a modified copy of a tree node by replacing its children.
    // to do so it instantiates the class with the new values assuming it will 'replay' the creation.
    // as any child might be also a field the method uses the following convention:
    //
    // 1. the children are created through constructor alone
    // 2. any children referenced through fields are also present in the children list
    // 3. the list of children is created through the constructor
    // 4. all the constructor arguments are available on the given instance through public methods using the same name.
    //
    // As an example:
    //
    // class Add extends TreeNode<T> {
    //     private Literal left;
    //     private Literal right;
    // 
    //     public Add(Literal left, Literal right) {
    //        this.left = left;
    //        this.right = right;
    //     }
    //
    //     public Literal left() { return left; }
    //     public Literal right() { return right; }
    // }
    static <T extends Node<T>> T copyTree(Node<T> tree, List<T> newChildren) {
        Assert.notNull(tree, "Non-null tree expected");

        // basic sanity check
        List<T> currentChildren = tree.children();
        Assert.isTrue(currentChildren.size() == newChildren.size(), "Cannot make copy; expected %s children but received %s", currentChildren.size(), newChildren.size()); 

        NodeInfo info = info(tree.getClass());
        Object[] props = properties(tree, info);

        // for each parameter, look in the list of children to find it
        // if it's missing, it's added as is, otherwise it gets picked up from the new ones 
        for (int i = 0; i < props.length; i++) {
            Object property = props[i];

            // in the rare case (UnresolvedFunction) the children are specified, copy them directly in the constructor
            if (i == info.childrenIndex) {
                props[i] = newChildren;
            }
            // check children only if needed
            else if (property instanceof Node) {
                // as the same instances are inside the children, an identity check is done instead of the usual equals
                for (int childIndex = 0; childIndex < currentChildren.size(); childIndex++) {
                    T child = currentChildren.get(childIndex);
                    // replace old property with the new one
                    if (property == child) {
                        props[i] = newChildren.get(childIndex);
                        // skip the rest of the children, if there are duplicates, will find them on their turn
                        break;
                    }
                }
            }
        }

        return cloneNode(tree, props);
    }


    @SuppressWarnings("unchecked")
    static <T extends Node<T>> T cloneNode(Node<T> tree, Object[] props) {
        NodeInfo treeNodeInfo = info(tree.getClass());

        // finally invoke the constructor and return the new copy
        try {
            return (T) treeNodeInfo.ctr.newInstance(props);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            throw new SqlIllegalArgumentException("Cannot call constructor %s to copy tree node",
                    treeNodeInfo.ctr.toGenericString(), ex);
        }
    }
    
    @SuppressWarnings("rawtypes")
    public static NodeInfo info(Class<? extends Node> clazz) {
        NodeInfo treeNodeInfo = CACHE.get(clazz);

        // perform discovery (and cache it)
        if (treeNodeInfo == null) {
            Constructor<?>[] constructors = clazz.getConstructors();
            Assert.isTrue(!ObjectUtils.isEmpty(constructors), "No public constructors found for class %s", clazz);
            
            // find the longest constructor
            Constructor<?> ctr = null;
            int maxParameterCount = -1;
            for (Constructor<?> constructor : constructors) {
                if (ctr == null || maxParameterCount < constructor.getParameterCount()) {
                    ctr = constructor;
                    maxParameterCount = constructor.getParameterCount();
                }
            }

            int childrenIndex = -1;

            Map<String, Method> params = new LinkedHashMap<>(ctr.getParameterCount());

            // find each argument in the ctr and find its relevant method/getter
            Parameter[] parameters = ctr.getParameters();
            for (int paramIndex = 0; paramIndex < parameters.length; paramIndex++) {
                Parameter param = parameters[paramIndex];
                // NOCOMMIT - oh boy. this is worth digging into. I suppose we preserve these for now but I don't think this is safe to rely on.
                Assert.isTrue(param.isNamePresent(), "Can't find constructor parameter names for [%s]. Is class debug information available?", 
                                clazz.toGenericString());
                String paramName = param.getName();

                if (paramName.equals("children")) {
                    childrenIndex = paramIndex;
                }
                // find getter for it
                Method getter = null;
                try {
                    getter = clazz.getMethod(paramName);
                } catch (NoSuchMethodException nsme) {
                    throw new SqlIllegalArgumentException("class [%s] expected to have method [%s] for retrieving constructor arguments; none found",
                            clazz.getName(), paramName);
                }
                
                // validate return type
                Class<?> expected = param.getType();
                Class<?> found = getter.getReturnType();
                Assert.isTrue(expected.isAssignableFrom(found), "Constructor param [%s] in class [%s] has type [%s] but found getter [%s]", paramName, clazz, expected, getter.toGenericString());
                
                params.put(paramName, getter);
            }
            
            treeNodeInfo = new NodeInfo(ctr, params, childrenIndex);
            CACHE.put(clazz, treeNodeInfo);
        }

        return treeNodeInfo;
    }

    public static Map<String, Object> propertiesMap(Node<?> tree) {
        NodeInfo info = info(tree.getClass());
        Object[] results = properties(tree, info);

        Map<String, Object> props = new LinkedHashMap<>(results.length);

        int index = 0;
        for (String name : info.params.keySet()) {
            props.put(name, results[index++]);
        }
        return props;
    }

    public static Object[] properties(Node<?> tree) {
        return properties(tree, info(tree.getClass()));
    }

    // minor optimization to avoid double map lookup inside this class
    private static Object[] properties(Node<?> tree, NodeInfo info) {
        Object[] props = new Object[info.params.size()];
        int copyIndex = 0;

        for (Entry<String, Method> param : info.params.entrySet()) {
            Method getter = param.getValue();
            Object result;
            try {
                result = getter.invoke(tree);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
                throw new SqlIllegalArgumentException("Cannot invoke method [%s]", getter.toGenericString(), ex);
            }

            props[copyIndex++] = result;
        }

        return props;
    }

    public static String propertiesToString(Node<?> tree, boolean skipIfChild) {
        StringBuilder sb = new StringBuilder();

        NodeInfo info = info(tree.getClass());
        Set<String> keySet = info.params.keySet();
        Object[] properties = properties(tree, info);

        List<?> children = tree.children();
        // eliminate children (they are rendered as part of the tree)
        int maxProperties = TO_STRING_MAX_PROP;
        Iterator<String> nameIterator = keySet.iterator();
        boolean needsComma = false;

        for (int i = 0; i < properties.length; i++) {
            Object object = properties[i];
            String propertyName = nameIterator.next();
            // consider a property if it is not ignored AND
            // it's not a child (optional)
            if (!TO_STRING_IGNORE_PROP.equals(propertyName) && !(skipIfChild && children.contains(object))) {
                if (maxProperties-- < 0) {
                    sb.append(format(Locale.ROOT, "...%s fields not shown", properties.length - TO_STRING_MAX_PROP));
                    break;
                }

                if (needsComma) {
                    sb.append(",");
                }
                sb.append(Objects.toString(object));

                needsComma = true;
            }
        }

        return sb.toString();
    }

    static String nodeString(Node<?> treeNode) {
        StringBuilder sb = new StringBuilder();
        sb.append(treeNode.nodeName());
        sb.append("[");
        sb.append(propertiesToString(treeNode, true));
        sb.append("]");
        return sb.toString();
    }

    static String toString(Node<?> treeNode) {
        return treeString(treeNode, new StringBuilder(), 0, new BitSet()).toString();
    }

    static <T extends Node<T>> StringBuilder treeString(Node<T> treeNode, StringBuilder sb, int depth, BitSet hasParentPerDepth) {
        if (depth > 0) {
            // draw children
            for (int column = 0; column < depth; column++) {
                if (hasParentPerDepth.get(column)) {
                    sb.append("|");
                    // if not the last elder, adding padding (since each column has two chars ("|_" or "\_")
                    if (column < depth - 1) {
                        sb.append(" ");
                    }
                }
                else {
                    // if the child has no parent (elder on the previous level), it means its the last sibling
                    sb.append((column == depth - 1) ? "\\" : "  ");
                }
            }

            sb.append("_");
        }
        
        if (treeNode == null) {
            sb.append("null");
            return sb;
        }

        // TreeNode by name (to allow nodes to override their expression)
        sb.append(treeNode.nodeString());

        List<T> children = treeNode.children();
        if (!children.isEmpty()) {
            sb.append("\n");
        }
        for (int i = 0; i < children.size(); i++) {
            T t = children.get(i);
            hasParentPerDepth.set(depth, i < children.size() - 1);
            treeString(t, sb, depth + 1, hasParentPerDepth);
            if (i < children.size() - 1) {
                sb.append("\n");
            }
        }
        return sb;
    }

    public static <A extends Node<A>, B extends Node<B>> String diffString(A left, B right) {
        return diffString(left.toString(), right.toString());
    }

    public static String diffString(String left, String right) {
        // break the strings into lines
        // then compare each line
        String[] leftSplit = left.split("\\n");
        String[] rightSplit = right.split("\\n");

        // find max - we could use streams but autoboxing is not cool
        int leftMaxPadding = 0;
        for (String string : leftSplit) {
            leftMaxPadding = Math.max(string.length(), leftMaxPadding);
        }
        
        // try to allocate the buffer - 5 represents the column comparison chars
        StringBuilder sb = new StringBuilder(left.length() + right.length() + Math.max(left.length(),  right.length()) * 3);

        boolean leftAvailable = true, rightAvailable = true;
        for (int leftIndex = 0, rightIndex = 0; leftAvailable || rightAvailable; leftIndex++, rightIndex++) {
            String leftRow = "", rightRow = leftRow;
            if (leftIndex < leftSplit.length) {
                leftRow = leftSplit[leftIndex];
            }
            else {
                leftAvailable = false;
            }
            sb.append(leftRow);
            for (int i = leftRow.length(); i < leftMaxPadding; i++) {
                sb.append(" ");
            }
            // right side still available
            if (rightIndex < rightSplit.length) {
                rightRow = rightSplit[rightIndex];
            }
            else {
                rightAvailable = false;
            }
            if (leftAvailable || rightAvailable) {
                sb.append(leftRow.equals(rightRow) ? " = " : " ! ");
                sb.append(rightRow);
                sb.append("\n");
            }
        }
        return sb.toString();
    }
}