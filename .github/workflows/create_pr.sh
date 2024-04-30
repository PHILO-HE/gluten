#!/bin/bash

yesterday=`date -d '-1 day' '+%Y_%m_%d'`

today=`date '+%Y_%m_%d'`

dayonly=`date '+%m-%d'`

curdir=$PWD

rebase_branch=${today}

# auto rebase velox
cd /opt
git clone -b main https://github.com/facebookincubator/velox
cd velox/
#git clean -dffx
#git reset --hard
#git rebase --abort || true

#git fetch origin
git remote add oap https://github.com/oap-project/velox
git fetch oap

# Force push $yesterday branch (least recently rebased branch) to oap/update.
# Skip if it doesn't exist (e.g., no rebase work was done yesterday).
if git rev-parse --quiet --verify oap/$yesterday 2>/dev/null; then
   echo "Updating oap/update branch.."
   git push oap +oap/$yesterday:update
fi

# Last rebase branch.
git checkout oap/update

# Get upstream commits since last rebase.
most_recent_common_ancester=$(git merge-base oap/update origin/main)
upstream_new_commits=$(git log $most_recent_common_ancester..origin/main --pretty=format:"%h by %an, %s")

# Fix json parse error.
# Replace \n with \\\n.
upstream_new_commits="${upstream_new_commits//$'\n'/\\\n}"
# Replace " with \\\".
upstream_new_commits="${upstream_new_commits//\"/\\\"}"
# Remove # symbol to avoid wrong PR reference in Github.
upstream_new_commits="${upstream_new_commits//#/}"

git rebase origin/main --verbose
if [ $? -ne 0 ]; then
    echo "Failed to rebase Velox."
    # Exit if failOnError arg is provided. Otherwise, go to the next steps.
    if [[ "$1" == "failOnError" ]]; then
      exit 1
    fi
fi

git checkout -b ${rebase_branch}
git add .
git ci -s -m "Rebase velox ($today)"
# Allow push failure if the branch exists (may have been pushed to bot velox repo mannually)
git push oap

# auto patch gluten
#cd /home/sparkuser/git/gluten
git clone https://github.com/apache/incubator-gluten.git
#git fetch origin
cd incubator-gluten/
git checkout origin/main -b ${rebase_branch}
sed -i "s/VELOX_BRANCH=2024.*/VELOX_BRANCH=${rebase_branch}/" ep/build-velox/src/get_velox.sh
git add ep
git ci -s -m "[VL] Daily Update Velox Version ($today)"
git remote add bot https://github.com/GlutenPerfBot/gluten
git push bot

# create gluten pull request
#--proxy http://proxy-shz.intel.com:911 \

get_data() {
 cat <<EOF
{
   "title": "[VL] Daily Update Velox Version ($today)",
   "head": "GlutenPerfBot:${rebase_branch}",
   "base": "main",
   "body": "Upstream Velox's New Commits:\n\n\`\`\`txt\n${upstream_new_commits}\n\`\`\`"
}
EOF
}

curl -L \
  -X POST \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  -H "Content-Type:application/json" \
  https://api.github.com/repos/apache/incubator-gluten/pulls \
  --data "$(get_data)"

