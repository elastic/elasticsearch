package org.elasticsearch.xpack.enrich;

public class EnrichPolicyResult {
    boolean completed;

    public EnrichPolicyResult(boolean completed) {
        this.completed = completed;
    }

    public boolean isCompleted() {
        return completed;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }
}
