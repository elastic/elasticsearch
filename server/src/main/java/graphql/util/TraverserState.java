package graphql.util;

import graphql.Internal;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static graphql.Assert.assertNotNull;

@Internal
public abstract class TraverserState<T> {

    private Object sharedContextData;

    private final Deque<Object> state;
    private final Set<T> visited = new LinkedHashSet<>();


    // used for depth first traversal
    private static class StackTraverserState<U> extends TraverserState<U> {

        private StackTraverserState(Object sharedContextData) {
            super(sharedContextData);
        }

        @Override
        public void pushAll(TraverserContext<U> traverserContext, Function<? super U, Map<String, ? extends List<U>>> getChildren) {
            super.state.push(traverserContext);

            EndList<U> endList = new EndList<>();
            super.state.push(endList);
            Map<String, List<TraverserContext<U>>> childrenContextMap = new LinkedHashMap<>();

            if (!traverserContext.isDeleted()) {
                Map<String, ? extends List<U>> childrenMap = getChildren.apply(traverserContext.thisNode());
                childrenMap.keySet().forEach(key -> {
                    List<U> children = childrenMap.get(key);
                    for (int i = children.size() - 1; i >= 0; i--) {
                        U child = assertNotNull(children.get(i), "null child for key " + key);
                        NodeLocation nodeLocation = new NodeLocation(key, i);
                        DefaultTraverserContext<U> context = super.newContext(child, traverserContext, nodeLocation);
                        super.state.push(context);
                        childrenContextMap.computeIfAbsent(key, notUsed -> new ArrayList<>());
                        childrenContextMap.get(key).add(0, context);
                    }
                });
            }
            endList.childrenContextMap = childrenContextMap;
        }
    }

    // used for breadth first traversal
    private static class QueueTraverserState<U> extends TraverserState<U> {

        private QueueTraverserState(Object sharedContextData) {
            super(sharedContextData);
        }

        @Override
        public void pushAll(TraverserContext<U> traverserContext, Function<? super U, Map<String, ? extends List<U>>> getChildren) {
            Map<String, List<TraverserContext<U>>> childrenContextMap = new LinkedHashMap<>();
            if (!traverserContext.isDeleted()) {
                Map<String, ? extends List<U>> childrenMap = getChildren.apply(traverserContext.thisNode());
                childrenMap.keySet().forEach(key -> {
                    List<U> children = childrenMap.get(key);
                    for (int i = 0; i < children.size(); i++) {
                        U child = assertNotNull(children.get(i), "null child for key " + key);
                        NodeLocation nodeLocation = new NodeLocation(key, i);
                        DefaultTraverserContext<U> context = super.newContext(child, traverserContext, nodeLocation);
                        childrenContextMap.computeIfAbsent(key, notUsed -> new ArrayList<>());
                        childrenContextMap.get(key).add(context);
                        super.state.add(context);
                    }
                });
            }
            EndList<U> endList = new EndList<>();
            endList.childrenContextMap = childrenContextMap;
            super.state.add(endList);
            super.state.add(traverserContext);
        }
    }

    public static class EndList<U> {
        public Map<String, List<TraverserContext<U>>> childrenContextMap;
    }

    private TraverserState(Object sharedContextData) {
        this.sharedContextData = sharedContextData;
        this.state = new ArrayDeque<>(32);
    }

    public static <U> TraverserState<U> newQueueState(Object sharedContextData) {
        return new QueueTraverserState<>(sharedContextData);
    }

    public static <U> TraverserState<U> newStackState(Object sharedContextData) {
        return new StackTraverserState<>(sharedContextData);
    }

    public abstract void pushAll(TraverserContext<T> o, Function<? super T, Map<String, ? extends List<T>>> getChildren);

    public Object pop() {
        return this.state.pop();
    }


    public void addNewContexts(Collection<? extends T> children, TraverserContext<T> parentContext) {
        assertNotNull(children).stream().map((child) -> newContext(child, parentContext, null)).forEach(this.state::add);
    }

    public boolean isEmpty() {
        return state.isEmpty();
    }


    public void addVisited(T visited) {
        this.visited.add(visited);
    }


    public DefaultTraverserContext<T> newRootContext(Map<Class<?>, Object> vars) {
        return newContextImpl(null, null, vars, null, true);
    }

    private DefaultTraverserContext<T> newContext(T o, TraverserContext<T> parent, NodeLocation position) {
        return newContextImpl(o, parent, new LinkedHashMap<>(), position, false);
    }

    private DefaultTraverserContext<T> newContextImpl(T curNode,
                                                      TraverserContext<T> parent,
                                                      Map<Class<?>, Object> vars,
                                                      NodeLocation nodeLocation,
                                                      boolean isRootContext) {
        assertNotNull(vars);
        return new DefaultTraverserContext<>(curNode, parent, visited, vars, sharedContextData, nodeLocation, isRootContext);
    }
}
