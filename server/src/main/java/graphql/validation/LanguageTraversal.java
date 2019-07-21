package graphql.validation;


import java.util.ArrayList;
import java.util.List;

import graphql.Internal;
import graphql.language.Node;

@Internal
public class LanguageTraversal {

    private final List<Node> path;

    public LanguageTraversal() {
        path = new ArrayList<>();
    }

    public LanguageTraversal(List<Node> basePath) {
        if (basePath != null) {
            path = basePath;
        } else {
            path = new ArrayList<>();
        }
    }

    public void traverse(Node root, DocumentVisitor documentVisitor) {
        traverseImpl(root, documentVisitor, path);
    }


    private void traverseImpl(Node<?> root, DocumentVisitor documentVisitor, List<Node> path) {
        documentVisitor.enter(root, path);
        path.add(root);
        List<Node> children = root.getChildren();
        for (Node child : children) {
            if (child != null) {
                traverseImpl(child, documentVisitor, path);
            }
        }
        path.remove(path.size() - 1);
        documentVisitor.leave(root, path);
    }
}
