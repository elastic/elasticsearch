#!/bin/bash
set -e
set -u
set -o pipefail

# This script creates a new branch then migrate all the maven stuff in it and commit that branch.
# then it runs mvn install to check that everything is still running

# Useful commands while testing
# rm -rf migration/tmp plugins
# git checkout pr/migration_script -f
# git add .; git commit -m "Add migration script" --amend
# ./migrate.sh 
# mvn clean install -DskipTests

DIR_TMP="../migration"
MODULE_CORE="core"
GIT_BRANCH="refactoring/maven"
PARENT_NAME="elasticsearch-parent"
PARENT_GIT="https://github.com/elastic/elasticsearch-parent.git"

# Insert a new text after a given line
# insertLinesAfter text_to_find text_to_add newLine_separator filename
# insertLinesAfter "<\/project>" "   <modules>=   <\/modules>" "§" "pom.xml"
function insertLinesAfter() {
	# echo "## modify $4 with $2"
	sed "s/$1/$1$3$2/" $4 | tr "$3" "\n" > $4.new
	mv $4.new $4
}

# Insert a new text before a given line
# insertLinesBefore text_to_find text_to_add newLine_separator filename
# insertLinesBefore "<\/project>" "   <modules>=   <\/modules>" "§" "pom.xml"
function insertLinesBefore() {
	# echo "## modify $4 with $2"
	sed "s/$1/$2$3$1/" $4 | tr "$3" "\n" > $4.new
	mv $4.new $4
}

# Replace text in a file
# replaceLine old_text new_text filename
# replaceLine "old" "new" "pom.xml"
function replaceLine() {
	# echo "## modify $3 with $2"
	sed "s/^$1/$2/" $3 > $3.new
	mv $3.new $3
}

# Remove lines in a file
# removeLines from_text_included to_text_included filename
# removeLines "old" "new" "pom.xml"
function removeLines() {
	# echo "## remove lines in $3 from $1 to $2"
	sed "/$1/,/$2/d" $3 > $3.new
	mv $3.new $3
}

# Migrate a plugin
# migratePlugin short_name
function migratePlugin() {
	PLUGIN_GIT_REPO="https://github.com/elastic/elasticsearch-$1.git"
	git remote add remote_$1 $PLUGIN_GIT_REPO
	git fetch remote_$1 > /dev/null 2>/dev/null
	echo "## Add $1 module from $PLUGIN_GIT_REPO"

	# echo "### fetch $1 project from $PLUGIN_GIT_REPO in plugins"
	# If you want to run that locally, uncomment this line and comment one below
	#cp -R ../elasticsearch-$1/* plugins/$1
	# git clone $PLUGIN_GIT_REPO plugins/$1 > /dev/null 2>/dev/null
	git checkout -b migrate_$1 remote_$1/master > /dev/null 2>/dev/null
	git remote rm remote_$1 > /dev/null 2>/dev/null
	mkdir -p plugins/$1
	git mv -k * plugins/$1 > /dev/null 2>/dev/null       
	git rm .gitignore > /dev/null 2>/dev/null
	# echo "### change $1 groupId to org.elasticsearch.plugins"
	# Change the groupId to avoid conflicts with existing 2.0.0 versions.
	replaceLine "    <groupId>org.elasticsearch<\/groupId>" "    <groupId>org.elasticsearch.plugin<\/groupId>" "plugins/$1/pom.xml"

	# echo "### cleanup $1 pom.xml"
	removeLines "<issueManagement>" "<\/issueManagement>" "plugins/$1/pom.xml"
	removeLines "<repositories>" "<\/repositories>" "plugins/$1/pom.xml"
	removeLines "<url>" "<\/scm>" "plugins/$1/pom.xml"

	# echo "### remove version 3.0.0-SNAPSHOT from $1 pom.xml"
	# All plugins for ES 2.0.0 uses 3.0.0-SNAPSHOT version number
	replaceLine "    <version>3.0.0-SNAPSHOT<\/version>" "" "plugins/$1/pom.xml"

	# echo "### remove unused dev-tools and .git dirs and LICENSE.txt and CONTRIBUTING.md files"
	rm -r plugins/$1/dev-tools
	rm -rf plugins/$1/.git
	rm plugins/$1/LICENSE.txt
	rm plugins/$1/CONTRIBUTING.md

	# echo "### commit changes"
	git add --all .
	git commit -m "add $1 module" > /dev/null
	git checkout $GIT_BRANCH
	git merge -m "migrate branch for $1" migrate_$1 > /dev/null 2>/dev/null
	# We can now add plugin module
	insertLinesBefore "    <\/modules>" "        <module>$1<\/module>" "§" "plugins/pom.xml"
	git add .
	git commit -m "add $1 module" > /dev/null
	git branch -D migrate_$1
}

echo "# STEP 0 : prepare the job"

# echo "## clean $DIR_TMP plugins dev-tools/target target"
rm -rf $DIR_TMP 
rm -rf plugins 
rm -rf dev-tools/target
rm -rf target

# echo "## create git $GIT_BRANCH work branch"

# It first clean the existing branch if any
echo "(You can safely ignore branch not found below)"
git branch -D $GIT_BRANCH > /dev/null || :

# Create the new branch
git branch $GIT_BRANCH > /dev/null
git checkout $GIT_BRANCH > /dev/null 2>/dev/null

echo "# STEP 1 : Core module"

# create the tmp work dir
# echo "## create $DIR_TMP temporary dir"
mkdir $DIR_TMP

# create the core module
# echo "## create core module in $MODULE_CORE"
rm -rf $MODULE_CORE 
mkdir $MODULE_CORE
# echo "## create $MODULE_CORE pom.xml"
cp pom.xml $MODULE_CORE
# echo "## modify $MODULE_CORE/pom.xml"
# We move <parent></parent> block on top
removeLines "<parent>" "<\/parent>" "$MODULE_CORE/pom.xml"
insertLinesAfter "<\/modelVersion>" "    <parent>§        <groupId>org.elasticsearch<\/groupId>§        <artifactId>elasticsearch-parent<\/artifactId>§        <version>2.0.0-SNAPSHOT<\/version>§    <\/parent>§" "§" "$MODULE_CORE/pom.xml"
# We clean useless data
replaceLine "    <version>2.0.0-SNAPSHOT<\/version>" "" "$MODULE_CORE/pom.xml"
removeLines "<inceptionYear>" "<\/scm>" "$MODULE_CORE/pom.xml"
removeLines "<repositories>" "<\/repositories>" "$MODULE_CORE/pom.xml"

# echo "## move src in $MODULE_CORE"
git mv src/ $MODULE_CORE
# echo "## move bin in $MODULE_CORE"
git mv bin/ $MODULE_CORE
# echo "## move config in $MODULE_CORE"
git mv config/ $MODULE_CORE 
# echo "## move lib in $MODULE_CORE"
git mv lib/ $MODULE_CORE 
# echo "## copy README.textile, LICENSE.txt and NOTICE.txt in $MODULE_CORE"
cp README.textile $MODULE_CORE 
cp LICENSE.txt $MODULE_CORE 
cp NOTICE.txt $MODULE_CORE 
# echo "## modify rest-api-spec location in $MODULE_CORE/pom.xml"
replaceLine "                <directory>\${project.basedir}\/rest-api-spec<\/directory>" "                <directory>\${project.basedir}\/..\/rest-api-spec<\/directory>" "$MODULE_CORE/pom.xml"


# echo "## commit changes"
git add .
git commit -m "create $MODULE_CORE module" > /dev/null

echo "# STEP 2 : Parent pom.xml from $PARENT_GIT"

# echo "## fetch parent project from $PARENT_GIT in $DIR_TMP"
# If you want to run that locally, uncomment this line and comment one below
# cp -R ../elasticsearch-parent $DIR_TMP
git clone $PARENT_GIT $DIR_TMP/$PARENT_NAME > /dev/null 2>/dev/null

cp $DIR_TMP/$PARENT_NAME/pom.xml .
cp -R $DIR_TMP/$PARENT_NAME/dev-tools .
cp -R $DIR_TMP/$PARENT_NAME/plugins .

# echo "## commit changes"
git add .
git commit -m "create parent pom project from its original location" > /dev/null

echo "# STEP 3 : Add $MODULE_CORE module to pom.xml"

insertLinesBefore "    <\/modules>" "        <module>$MODULE_CORE<\/module>" "§" "pom.xml"

# echo "## change name to Elasticsearch Core"
replaceLine "    <name>Elasticsearch core<\/name>" "    <name>Elasticsearch Core<\/name>" "$MODULE_CORE/pom.xml"

# echo "## commit changes"
git add .
git commit -m "add core module" > /dev/null

echo "# STEP 4 : Migrate plugins"

# We need to add <modules></modules> in the plugins parent project as it does not exist
insertLinesBefore "<\/project>" "    <modules>§    <\/modules>" "§" "plugins/pom.xml"
git add plugins/pom.xml
git commit -m "add modules section"
# Analysis
migratePlugin "analysis-kuromoji" 
migratePlugin "analysis-smartcn"
migratePlugin "analysis-stempel"
migratePlugin "analysis-phonetic"
migratePlugin "analysis-icu"

# Mapper
# TODO: look at this one later
# migratePlugin "mapper-attachments"

# Cloud
migratePlugin "cloud-gce"
migratePlugin "cloud-azure"
migratePlugin "cloud-aws"

echo "# STEP 5 : Clean tmp dir"

# echo "## clean $DIR_TMP"
rm -rf $DIR_TMP 

echo "you can now run:"
echo "mvn clean install -DskipTests"


