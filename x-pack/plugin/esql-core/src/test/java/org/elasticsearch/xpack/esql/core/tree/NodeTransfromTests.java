/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.tree;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.core.tree.SourceTests.randomSource;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class NodeTransfromTests extends ESTestCase {
    // Transform Up Tests
    public void testTransformUpSimpleLeafTransformation() throws Exception {
        NodeTests.ChildrenAreAProperty tree = createSimpleTree();
        Function<NodeTests.Dummy, NodeTests.Dummy> transformer = createLeafTransformer();

        NodeTests.Dummy result = tree.transformUp(transformer);

        assertThat(result, instanceOf(NodeTests.ChildrenAreAProperty.class));
        NodeTests.ChildrenAreAProperty transformed = (NodeTests.ChildrenAreAProperty) result;
        assertThat(transformed.children().get(0).thing(), equalTo("leaf1_transformed"));
        assertThat(transformed.children().get(1).thing(), equalTo("leaf2_transformed"));

        // Verify async version matches
        assertAsyncTransformMatches(tree, transformer, result);
    }

    public void testTransformUpWithTypeToken() throws Exception {
        NodeTests.ChildrenAreAProperty tree = createSimpleTree();
        Function<NodeTests.NoChildren, NodeTests.Dummy> transformer = n -> new NodeTests.NoChildren(n.source(), n.thing() + "_transformed");

        NodeTests.Dummy result = tree.transformUp(NodeTests.NoChildren.class, transformer);

        assertThat(result, instanceOf(NodeTests.ChildrenAreAProperty.class));
        NodeTests.ChildrenAreAProperty transformed = (NodeTests.ChildrenAreAProperty) result;
        assertThat(transformed.children().get(0).thing(), equalTo("leaf1_transformed"));
        assertThat(transformed.children().get(1).thing(), equalTo("leaf2_transformed"));

        // Verify async version matches
        SetOnce<NodeTests.Dummy> asyncResult = new SetOnce<>();
        tree.transformUp(
            NodeTests.NoChildren.class,
            (n, listener) -> listener.onResponse(transformer.apply(n)),
            ActionListener.wrap(asyncResult::set, ESTestCase::fail)
        );
        assertBusy(() -> assertThat(asyncResult.get(), equalTo(result)));
    }

    public void testTransformUpWithPredicate() throws Exception {
        NodeTests.ChildrenAreAProperty tree = createSimpleTree();
        Predicate<Node<?>> predicate = n -> n instanceof NodeTests.NoChildren && ((NodeTests.NoChildren) n).thing().equals("leaf1");
        Function<NodeTests.Dummy, NodeTests.Dummy> transformer = n -> new NodeTests.NoChildren(n.source(), n.thing() + "_transformed");

        NodeTests.Dummy result = tree.transformUp(predicate, transformer);

        assertThat(result, instanceOf(NodeTests.ChildrenAreAProperty.class));
        NodeTests.ChildrenAreAProperty transformed = (NodeTests.ChildrenAreAProperty) result;
        assertThat(transformed.children().get(0).thing(), equalTo("leaf1_transformed"));
        assertThat(transformed.children().get(1).thing(), equalTo("leaf2")); // Not transformed

        // Verify async version matches
        SetOnce<NodeTests.Dummy> asyncResult = new SetOnce<>();
        tree.transformUp(
            predicate,
            (n, listener) -> listener.onResponse(transformer.apply(n)),
            ActionListener.wrap(asyncResult::set, ESTestCase::fail)
        );
        assertBusy(() -> assertThat(asyncResult.get(), equalTo(result)));
    }

    public void testTransformUpErrorHandling() {
        NodeTests.ChildrenAreAProperty tree = createSimpleTree();

        RuntimeException e = expectThrows(
            RuntimeException.class,
            () -> tree.transformUp(n -> { throw new RuntimeException("test error"); })
        );
        assertThat(e.getMessage(), equalTo("test error"));
    }

    public void testTransformUpAsyncErrorHandling() throws Exception {
        NodeTests.ChildrenAreAProperty tree = createSimpleTree();
        SetOnce<RuntimeException> exception = new SetOnce<>();

        tree.transformUp(
            (n, l) -> l.onFailure(new RuntimeException("test error")),
            ActionListener.wrap(r -> fail("should not be called"), e -> exception.set(asInstanceOf(RuntimeException.class, e)))
        );

        assertBusy(() -> assertThat(exception.get().getMessage(), equalTo("test error")));
    }

    public void testTransformUpNestedStructures() throws Exception {
        NodeTests.ChildrenAreAProperty tree = createNestedTree();
        Function<NodeTests.Dummy, NodeTests.Dummy> transformer = createAllNodesTransformer();

        NodeTests.Dummy result = tree.transformUp(transformer);

        assertThat(result, instanceOf(NodeTests.ChildrenAreAProperty.class));
        NodeTests.ChildrenAreAProperty transformedOuter = (NodeTests.ChildrenAreAProperty) result;
        assertThat(transformedOuter.thing(), equalTo("outer_transformed"));

        NodeTests.Dummy innerResult = transformedOuter.children().get(0);
        assertThat(innerResult, instanceOf(NodeTests.ChildrenAreAProperty.class));
        NodeTests.ChildrenAreAProperty transformedInner = (NodeTests.ChildrenAreAProperty) innerResult;
        assertThat(transformedInner.thing(), equalTo("inner_transformed"));
        assertThat(transformedInner.children().get(0).thing(), equalTo("leaf1_transformed"));
        assertThat(transformedInner.children().get(1).thing(), equalTo("leaf2_transformed"));

        // Verify async version matches
        assertAsyncTransformMatches(tree, transformer, result);
    }

    // Transform Down Tests
    public void testTransformDownSimpleLeafTransformation() throws Exception {
        NodeTests.ChildrenAreAProperty tree = createSimpleTree();
        Function<NodeTests.Dummy, NodeTests.Dummy> transformer = createLeafTransformer();

        NodeTests.Dummy result = tree.transformDown(transformer);

        assertThat(result, instanceOf(NodeTests.ChildrenAreAProperty.class));
        NodeTests.ChildrenAreAProperty transformed = (NodeTests.ChildrenAreAProperty) result;
        assertThat(transformed.children().get(0).thing(), equalTo("leaf1_transformed"));
        assertThat(transformed.children().get(1).thing(), equalTo("leaf2_transformed"));

        // Verify async version matches
        assertAsyncTransformDownMatches(tree, transformer, result);
    }

    public void testTransformDownWithTypeToken() throws Exception {
        NodeTests.ChildrenAreAProperty tree = createSimpleTree();
        Function<NodeTests.NoChildren, NodeTests.Dummy> transformer = n -> new NodeTests.NoChildren(n.source(), n.thing() + "_transformed");

        NodeTests.Dummy result = tree.transformDown(NodeTests.NoChildren.class, transformer);

        assertThat(result, instanceOf(NodeTests.ChildrenAreAProperty.class));
        NodeTests.ChildrenAreAProperty transformed = (NodeTests.ChildrenAreAProperty) result;
        assertThat(transformed.children().get(0).thing(), equalTo("leaf1_transformed"));
        assertThat(transformed.children().get(1).thing(), equalTo("leaf2_transformed"));

        // Verify async version matches
        SetOnce<NodeTests.Dummy> asyncResult = new SetOnce<>();
        tree.transformDown(
            NodeTests.NoChildren.class,
            (n, listener) -> listener.onResponse(transformer.apply(n)),
            ActionListener.wrap(asyncResult::set, ESTestCase::fail)
        );
        assertBusy(() -> assertThat(asyncResult.get(), equalTo(result)));
    }

    public void testTransformDownWithPredicate() throws Exception {
        NodeTests.ChildrenAreAProperty tree = createSimpleTree();
        Predicate<Node<?>> predicate = n -> n instanceof NodeTests.NoChildren && ((NodeTests.NoChildren) n).thing().equals("leaf1");
        Function<NodeTests.Dummy, NodeTests.Dummy> transformer = n -> new NodeTests.NoChildren(n.source(), n.thing() + "_transformed");

        NodeTests.Dummy result = tree.transformDown(predicate, transformer);

        assertThat(result, instanceOf(NodeTests.ChildrenAreAProperty.class));
        NodeTests.ChildrenAreAProperty transformed = (NodeTests.ChildrenAreAProperty) result;
        assertThat(transformed.children().get(0).thing(), equalTo("leaf1_transformed"));
        assertThat(transformed.children().get(1).thing(), equalTo("leaf2")); // Not transformed

        // Verify async version matches
        SetOnce<NodeTests.Dummy> asyncResult = new SetOnce<>();
        tree.transformDown(
            predicate,
            (n, listener) -> listener.onResponse(transformer.apply(n)),
            ActionListener.wrap(asyncResult::set, ESTestCase::fail)
        );
        assertBusy(() -> assertThat(asyncResult.get(), equalTo(result)));
    }

    public void testTransformDownErrorHandling() {
        NodeTests.ChildrenAreAProperty tree = createSimpleTree();

        RuntimeException e = expectThrows(
            RuntimeException.class,
            () -> tree.transformDown(n -> { throw new RuntimeException("test error"); })
        );
        assertThat(e.getMessage(), equalTo("test error"));
    }

    public void testTransformDownAsyncErrorHandling() throws Exception {
        NodeTests.ChildrenAreAProperty tree = createSimpleTree();
        SetOnce<RuntimeException> exception = new SetOnce<>();

        tree.transformDown((n, listener) -> {
            if (n instanceof NodeTests.NoChildren) {
                listener.onFailure(new RuntimeException("test error"));
            } else {
                listener.onResponse(n);
            }
        }, ActionListener.wrap(r -> fail("should not be called"), e -> exception.set(asInstanceOf(RuntimeException.class, e))));

        assertBusy(() -> {
            assertNotNull(exception.get());
            assertThat(exception.get().getMessage(), equalTo("test error"));
        });
    }

    public void testTransformDownNestedStructures() throws Exception {
        NodeTests.ChildrenAreAProperty tree = createNestedTree();
        Function<NodeTests.Dummy, NodeTests.Dummy> transformer = createAllNodesTransformer();

        NodeTests.Dummy result = tree.transformDown(transformer);

        assertThat(result, instanceOf(NodeTests.ChildrenAreAProperty.class));
        NodeTests.ChildrenAreAProperty transformedOuter = (NodeTests.ChildrenAreAProperty) result;
        assertThat(transformedOuter.thing(), equalTo("outer_transformed"));

        NodeTests.Dummy innerResult = transformedOuter.children().get(0);
        assertThat(innerResult, instanceOf(NodeTests.ChildrenAreAProperty.class));
        NodeTests.ChildrenAreAProperty transformedInner = (NodeTests.ChildrenAreAProperty) innerResult;
        assertThat(transformedInner.thing(), equalTo("inner_transformed"));
        assertThat(transformedInner.children().get(0).thing(), equalTo("leaf1_transformed"));
        assertThat(transformedInner.children().get(1).thing(), equalTo("leaf2_transformed"));

        // Verify async version matches
        assertAsyncTransformDownMatches(tree, transformer, result);
    }

    // Tests demonstrating behavioral differences between transformUp and transformDown
    public void testTransformUpVsDownOrderDependentTransformation() {
        NodeTests.NoChildren leaf1 = new NodeTests.NoChildren(randomSource(), "leaf");
        NodeTests.NoChildren leaf2 = new NodeTests.NoChildren(randomSource(), "leaf");
        NodeTests.ChildrenAreAProperty innerNode = new NodeTests.ChildrenAreAProperty(randomSource(), List.of(leaf1, leaf2), "inner");
        NodeTests.ChildrenAreAProperty outerNode = new NodeTests.ChildrenAreAProperty(randomSource(), List.of(innerNode), "outer");

        Function<NodeTests.Dummy, NodeTests.Dummy> transformerDown = n -> {
            if (n instanceof NodeTests.ChildrenAreAProperty) {
                NodeTests.ChildrenAreAProperty cn = (NodeTests.ChildrenAreAProperty) n;
                return new NodeTests.ChildrenAreAProperty(cn.source(), cn.children(), cn.thing() + "_DOWN");
            }
            return n;
        };

        Function<NodeTests.Dummy, NodeTests.Dummy> transformerUp = n -> {
            if (n instanceof NodeTests.ChildrenAreAProperty) {
                NodeTests.ChildrenAreAProperty cn = (NodeTests.ChildrenAreAProperty) n;
                return new NodeTests.ChildrenAreAProperty(cn.source(), cn.children(), cn.thing() + "_UP");
            }
            return n;
        };

        // Transform down: parent first, then children
        NodeTests.Dummy resultDown = outerNode.transformDown(transformerDown);
        NodeTests.ChildrenAreAProperty outerDown = (NodeTests.ChildrenAreAProperty) resultDown;
        NodeTests.ChildrenAreAProperty innerDown = (NodeTests.ChildrenAreAProperty) outerDown.children().get(0);

        // Transform up: children first, then parent
        NodeTests.Dummy resultUp = outerNode.transformUp(transformerUp);
        NodeTests.ChildrenAreAProperty outerUp = (NodeTests.ChildrenAreAProperty) resultUp;
        NodeTests.ChildrenAreAProperty innerUp = (NodeTests.ChildrenAreAProperty) outerUp.children().get(0);

        // Verify transformation order is reflected in results
        assertThat(outerDown.thing(), equalTo("outer_DOWN"));
        assertThat(innerDown.thing(), equalTo("inner_DOWN"));
        assertThat(outerUp.thing(), equalTo("outer_UP"));
        assertThat(innerUp.thing(), equalTo("inner_UP"));
    }

    public void testTransformUpVsDownChildDependentLogic() {
        NodeTests.NoChildren leaf1 = new NodeTests.NoChildren(randomSource(), "A");
        NodeTests.NoChildren leaf2 = new NodeTests.NoChildren(randomSource(), "B");
        NodeTests.ChildrenAreAProperty node = new NodeTests.ChildrenAreAProperty(randomSource(), List.of(leaf1, leaf2), "parent");

        // Transformer that changes parent based on children's current state
        Function<NodeTests.Dummy, NodeTests.Dummy> transformer = n -> {
            if (n instanceof NodeTests.ChildrenAreAProperty) {
                NodeTests.ChildrenAreAProperty cn = (NodeTests.ChildrenAreAProperty) n;
                // Count how many children have "transformed" in their name
                long transformedChildrenCount = cn.children().stream().filter(child -> child.thing().contains("transformed")).count();
                return new NodeTests.ChildrenAreAProperty(
                    cn.source(),
                    cn.children(),
                    cn.thing() + "_sees_" + transformedChildrenCount + "_transformed_children"
                );
            } else if (n instanceof NodeTests.NoChildren) {
                return new NodeTests.NoChildren(n.source(), n.thing() + "_transformed");
            }
            return n;
        };

        // Transform down: parent sees children in original state
        NodeTests.Dummy resultDown = node.transformDown(transformer);
        NodeTests.ChildrenAreAProperty parentDown = (NodeTests.ChildrenAreAProperty) resultDown;

        // Transform up: parent sees children after they've been transformed
        NodeTests.Dummy resultUp = node.transformUp(transformer);
        NodeTests.ChildrenAreAProperty parentUp = (NodeTests.ChildrenAreAProperty) resultUp;

        // Key difference: transformDown parent sees 0 transformed children,
        // transformUp parent sees 2 transformed children
        assertThat(parentDown.thing(), equalTo("parent_sees_0_transformed_children"));
        assertThat(parentUp.thing(), equalTo("parent_sees_2_transformed_children"));

        // Both should have transformed children
        assertThat(parentDown.children().get(0).thing(), equalTo("A_transformed"));
        assertThat(parentDown.children().get(1).thing(), equalTo("B_transformed"));
        assertThat(parentUp.children().get(0).thing(), equalTo("A_transformed"));
        assertThat(parentUp.children().get(1).thing(), equalTo("B_transformed"));
    }

    public void testTransformUpVsDownConditionalTransformation() {
        NodeTests.NoChildren leaf1 = new NodeTests.NoChildren(randomSource(), "child1");
        NodeTests.NoChildren leaf2 = new NodeTests.NoChildren(randomSource(), "child2");
        NodeTests.ChildrenAreAProperty node = new NodeTests.ChildrenAreAProperty(randomSource(), List.of(leaf1, leaf2), "STOP");

        // Transformer that stops transformation if parent has "STOP" in name
        Function<NodeTests.Dummy, NodeTests.Dummy> transformer = n -> {
            if (n instanceof NodeTests.ChildrenAreAProperty) {
                NodeTests.ChildrenAreAProperty cn = (NodeTests.ChildrenAreAProperty) n;
                if (cn.thing().contains("STOP")) {
                    // Return node unchanged
                    return cn;
                } else {
                    return new NodeTests.ChildrenAreAProperty(cn.source(), cn.children(), cn.thing() + "_processed");
                }
            } else if (n instanceof NodeTests.NoChildren) {
                return new NodeTests.NoChildren(n.source(), n.thing() + "_transformed");
            }
            return n;
        };

        NodeTests.Dummy resultDown = node.transformDown(transformer);
        NodeTests.ChildrenAreAProperty parentDown = (NodeTests.ChildrenAreAProperty) resultDown;

        NodeTests.Dummy resultUp = node.transformUp(transformer);
        NodeTests.ChildrenAreAProperty parentUp = (NodeTests.ChildrenAreAProperty) resultUp;

        // Both parents should remain unchanged (contain "STOP")
        assertThat(parentDown.thing(), equalTo("STOP"));
        assertThat(parentUp.thing(), equalTo("STOP"));

        // Both should have transformed children
        assertThat(parentDown.children().get(0).thing(), equalTo("child1_transformed"));
        assertThat(parentUp.children().get(0).thing(), equalTo("child1_transformed"));
    }

    public void testTransformUpVsDownAccumulativeChanges() {
        NodeTests.NoChildren leaf = new NodeTests.NoChildren(randomSource(), "0");
        NodeTests.AChildIsAProperty innerNode = new NodeTests.AChildIsAProperty(randomSource(), leaf, "0");
        NodeTests.AChildIsAProperty outerNode = new NodeTests.AChildIsAProperty(randomSource(), innerNode, "0");

        // Transformer that increments numeric values
        Function<NodeTests.Dummy, NodeTests.Dummy> transformer = n -> {
            try {
                int currentValue = Integer.parseInt(n.thing());
                String newValue = String.valueOf(currentValue + 1);

                if (n instanceof NodeTests.NoChildren) {
                    return new NodeTests.NoChildren(n.source(), newValue);
                } else if (n instanceof NodeTests.AChildIsAProperty) {
                    NodeTests.AChildIsAProperty an = (NodeTests.AChildIsAProperty) n;
                    return new NodeTests.AChildIsAProperty(an.source(), an.child(), newValue);
                }
            } catch (NumberFormatException e) {
                // If not a number, leave unchanged
            }
            return n;
        };

        NodeTests.Dummy resultDown = outerNode.transformDown(transformer);
        NodeTests.Dummy resultUp = outerNode.transformUp(transformer);

        // Extract the final values
        NodeTests.AChildIsAProperty outerDown = (NodeTests.AChildIsAProperty) resultDown;
        NodeTests.AChildIsAProperty innerDown = (NodeTests.AChildIsAProperty) outerDown.child();
        NodeTests.NoChildren leafDown = (NodeTests.NoChildren) innerDown.child();

        NodeTests.AChildIsAProperty outerUp = (NodeTests.AChildIsAProperty) resultUp;
        NodeTests.AChildIsAProperty innerUp = (NodeTests.AChildIsAProperty) outerUp.child();
        NodeTests.NoChildren leafUp = (NodeTests.NoChildren) innerUp.child();

        // All nodes should be incremented to "1"
        assertThat(leafDown.thing(), equalTo("1"));
        assertThat(leafUp.thing(), equalTo("1"));
        assertThat(innerDown.thing(), equalTo("1"));
        assertThat(innerUp.thing(), equalTo("1"));
        assertThat(outerDown.thing(), equalTo("1"));
        assertThat(outerUp.thing(), equalTo("1"));
    }

    // Helper methods for transform tests
    private NodeTests.ChildrenAreAProperty createSimpleTree() {
        NodeTests.NoChildren leaf1 = new NodeTests.NoChildren(randomSource(), "leaf1");
        NodeTests.NoChildren leaf2 = new NodeTests.NoChildren(randomSource(), "leaf2");
        return new NodeTests.ChildrenAreAProperty(randomSource(), List.of(leaf1, leaf2), "node");
    }

    private NodeTests.ChildrenAreAProperty createNestedTree() {
        NodeTests.NoChildren leaf1 = new NodeTests.NoChildren(randomSource(), "leaf1");
        NodeTests.NoChildren leaf2 = new NodeTests.NoChildren(randomSource(), "leaf2");
        NodeTests.ChildrenAreAProperty innerNode = new NodeTests.ChildrenAreAProperty(randomSource(), List.of(leaf1, leaf2), "inner");
        return new NodeTests.ChildrenAreAProperty(randomSource(), List.of(innerNode), "outer");
    }

    private Function<NodeTests.Dummy, NodeTests.Dummy> createLeafTransformer() {
        return n -> n instanceof NodeTests.NoChildren ? new NodeTests.NoChildren(n.source(), n.thing() + "_transformed") : n;
    }

    private Function<NodeTests.Dummy, NodeTests.Dummy> createAllNodesTransformer() {
        return n -> {
            if (n instanceof NodeTests.NoChildren) {
                return new NodeTests.NoChildren(n.source(), ((NodeTests.NoChildren) n).thing() + "_transformed");
            } else if (n instanceof NodeTests.ChildrenAreAProperty) {
                NodeTests.ChildrenAreAProperty cn = (NodeTests.ChildrenAreAProperty) n;
                return new NodeTests.ChildrenAreAProperty(cn.source(), cn.children(), cn.thing() + "_transformed");
            }
            return n;
        };
    }

    private void assertAsyncTransformMatches(
        NodeTests.Dummy node,
        Function<NodeTests.Dummy, NodeTests.Dummy> transformer,
        NodeTests.Dummy expectedResult
    ) throws Exception {
        SetOnce<NodeTests.Dummy> asyncResult = new SetOnce<>();
        ((Node<NodeTests.Dummy>) node).transformUp(
            (n, listener) -> listener.onResponse(transformer.apply(n)),
            ActionListener.wrap(asyncResult::set, ESTestCase::fail)
        );
        assertBusy(() -> assertThat(asyncResult.get(), equalTo(expectedResult)));
    }

    private void assertAsyncTransformDownMatches(
        NodeTests.Dummy node,
        Function<NodeTests.Dummy, NodeTests.Dummy> transformer,
        NodeTests.Dummy expectedResult
    ) throws Exception {
        SetOnce<NodeTests.Dummy> asyncResult = new SetOnce<>();
        ((Node<NodeTests.Dummy>) node).transformDown(
            (n, listener) -> listener.onResponse(transformer.apply(n)),
            ActionListener.wrap(asyncResult::set, ESTestCase::fail)
        );
        assertBusy(() -> assertThat(asyncResult.get(), equalTo(expectedResult)));
    }
}
