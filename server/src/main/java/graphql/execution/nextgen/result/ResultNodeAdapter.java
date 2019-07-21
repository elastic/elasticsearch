package graphql.execution.nextgen.result;

import graphql.PublicApi;
import graphql.util.NodeAdapter;
import graphql.util.NodeLocation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static graphql.Assert.assertNotNull;
import static graphql.Assert.assertTrue;

@PublicApi
public class ResultNodeAdapter implements NodeAdapter<ExecutionResultNode> {

    public static final ResultNodeAdapter RESULT_NODE_ADAPTER = new ResultNodeAdapter();

    private ResultNodeAdapter() {

    }

    @Override
    public Map<String, List<ExecutionResultNode>> getNamedChildren(ExecutionResultNode parentNode) {
        Map<String, List<ExecutionResultNode>> result = new LinkedHashMap<>();
        result.put(null, parentNode.getChildren());
        return result;
    }

    @Override
    public ExecutionResultNode withNewChildren(ExecutionResultNode parentNode, Map<String, List<ExecutionResultNode>> newChildren) {
        assertTrue(newChildren.size() == 1);
        List<ExecutionResultNode> childrenList = newChildren.get(null);
        assertNotNull(childrenList);
        return parentNode.withNewChildren(childrenList);
    }

    @Override
    public ExecutionResultNode removeChild(ExecutionResultNode parentNode, NodeLocation location) {
        int index = location.getIndex();
        List<ExecutionResultNode> childrenList = parentNode.getChildren();
        assertTrue(index >= 0 && index < childrenList.size(), "The remove index MUST be within the range of the children");
        childrenList.remove(index);
        return parentNode.withNewChildren(childrenList);
    }
}
