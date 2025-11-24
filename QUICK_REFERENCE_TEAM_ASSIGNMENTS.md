# Quick Reference: Team Label Assignments for :Delivery/Build Issues

## Summary Statistics
- **Total Issues Analyzed:** 171
- **High-Confidence Reassignments Needed:** 21 issues
- **Medium-Confidence Reassignments:** 48 issues  
- **Remain with Team:Delivery:** 102 issues

## Immediate Action Items (High Confidence)

### Team:ML - 11 Issues to Reassign
```bash
# Test classes clearly in x-pack/plugin/ml module
gh issue edit 131016 --add-label "Team:ML" --remove-label "Team:Delivery"
gh issue edit 130974 --add-label "Team:ML" --remove-label "Team:Delivery"
gh issue edit 126001 --add-label "Team:ML" --remove-label "Team:Delivery"
gh issue edit 125906 --add-label "Team:ML" --remove-label "Team:Delivery"
gh issue edit 125905 --add-label "Team:ML" --remove-label "Team:Delivery"
gh issue edit 125904 --add-label "Team:ML" --remove-label "Team:Delivery"
gh issue edit 125903 --add-label "Team:ML" --remove-label "Team:Delivery"
gh issue edit 124826 --add-label "Team:ML" --remove-label "Team:Delivery"
gh issue edit 124160 --add-label "Team:ML" --remove-label "Team:Delivery"
gh issue edit 120864 --add-label "Team:ML" --remove-label "Team:Delivery"
gh issue edit 120476 --add-label "Team:ML" --remove-label "Team:Delivery"
```

### Team:Security - 7 Issues to Reassign
```bash
# Test classes clearly in x-pack/plugin/security module
gh issue edit 134094 --add-label "Team:Security" --remove-label "Team:Delivery"
gh issue edit 132364 --add-label "Team:Security" --remove-label "Team:Delivery"
gh issue edit 121848 --add-label "Team:Security" --remove-label "Team:Delivery"
gh issue edit 121847 --add-label "Team:Security" --remove-label "Team:Delivery"
gh issue edit 121846 --add-label "Team:Security" --remove-label "Team:Delivery"
gh issue edit 120435 --add-label "Team:Security" --remove-label "Team:Delivery"
gh issue edit 112635 --add-label "Team:Security" --remove-label "Team:Delivery"
```

### Team:ES|QL - 3 Issues to Reassign
```bash
# Test classes clearly in x-pack/plugin/sql module
gh issue edit 132249 --add-label "Team:ES|QL" --remove-label "Team:Delivery"
gh issue edit 130159 --add-label "Team:ES|QL" --remove-label "Team:Delivery"
gh issue edit 120440 --add-label "Team:ES|QL" --remove-label "Team:Delivery"
```

## Medium Confidence - Review with Teams

### Team:Search - 21 Issues
YAML tests for search features: sort, query, knn, vectors, analyzers
- Key issues: #138297, #133222, #132854, #131788, #131561
- **Action:** Review with Search team before reassigning

### Team:Data Management - 20 Issues
YAML tests for: indices, mappings, data streams, ILM, snapshots, ingest
- Key issues: #137493, #137457, #137158, #137155, #133884
- **Action:** Review with Data Management team before reassigning

### Team:Distributed Coordination - 2 Issues
- #135592 (CCR tests)
- #125901 (Rejection handling)

### Team:Distributed Indexing - 1 Issue
- #131793 (GlobalCheckpointSyncActionIT)

## Files Generated

1. **team_assignment_report.csv** - Full spreadsheet with all 171 issues
   - Columns: Issue #, Title, Current Team, Suggested Team, Confidence, Test Class, YAML Path, Feature Area, Reasoning, URL
   - Use for bulk updates or filtering

2. **team_assignment_analysis.md** - Complete analysis report
   - Detailed methodology
   - Full reasoning for each suggestion
   - Git history verification

3. **JSON files** (in /tmp/)
   - `/tmp/issues_with_teams.json` - Machine-readable analysis
   - `/tmp/issues_categorized.json` - Categorization data

## Methodology Used

1. **Test Class Analysis** - Identified module ownership from test file paths
2. **YAML Path Mapping** - Mapped REST API test paths to feature teams
3. **Git History** - Verified which teams actively work on related code
4. **CODEOWNERS** - Cross-referenced with established ownership patterns
5. **Module Structure** - Mapped x-pack plugins to teams

## Confidence Levels Explained

- **High (H):** Test class name/module clearly indicates team ownership
  - Example: `MlHiddenIndicesFullClusterRestartIT` → Team:ML
  
- **Medium (M):** YAML test path or feature area indicates ownership
  - Example: `reference/esql/esql-syntax/*` → Team:ES|QL
  
- **Low (L):** Generic test infrastructure, genuinely belongs to Delivery
  - Example: Gradle build configuration, YAML test framework

## Key Insights

1. **60% of issues legitimately belong to Team:Delivery**
   - Build system, CI/CD, test frameworks, packaging

2. **40% should be reassigned to feature teams**
   - 21 high-confidence (immediate action)
   - 48 medium-confidence (review with teams)

3. **YAML test runners are generic**
   - Must analyze YAML test path to determine feature being tested
   - `MixedClusterClientYamlTestSuiteIT` runs tests from many features

4. **Module structure is reliable for ownership**
   - `x-pack/plugin/ml` → Team:ML
   - `x-pack/plugin/security` → Team:Security
   - `x-pack/plugin/sql` → Team:ES|QL

## Next Steps

1. ✅ Execute high-confidence reassignments (21 issues)
2. ⚠️ Review medium-confidence with respective teams (48 issues)
3. ✓ Keep remaining issues with Team:Delivery (102 issues)

---
Generated: 2025-11-19
Total Issues: 171
Analysis Tools: GitHub CLI, Git history, CODEOWNERS, Module structure
