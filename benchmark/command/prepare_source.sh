#!/bin/bash
#　Spark 源码切换到branch-2.3 分支
git checkout branch-2.3

#Spark 源码编译
mvn -DskipTests clean install  -Dcheckstyle.skip=true
