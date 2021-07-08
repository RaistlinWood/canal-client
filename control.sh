#!/usr/bin/env bash
source /etc/profile

pid=0

PACKAGE_NAME="canal-client.jar"

JVM_ARGS= " -Xms2g -Xmx2g -XX:UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseCMSInitiatingOccupancyOnly -XX:-OmitStackTraceInFastThrow -XX:+HeapDumpOnOutOfMemoryError -Xloggc:logs/gc.log -XX: +PrintGCDetails -XX:+PrintReferenceGC -XX:+ParallelRefProcEnable -XX:+PrintGCDateStamps -XX:PrintGCTimeStamps"

if [ ${3}x = "debug"x ];then
    JVM_ARGS="${JVM_ARGS} -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,address=48793,suspend=n"
fi

PROFILE=""
if [ ! -n "${2}" ];then
    PROFILE="dev"
else
    PROFILE=$2
fi

start(){
    checkPid
    if [ "${pid}"x != "0"x ];then
        echo "please do not repeat "
        return 1
    fi
    checkJava
    if [ "${pid}"x != "0"x ];then
        echo "please do not repeat "
        return 1
    fi

    echo "starting ${PACKAGE_NAME} process"
    checkProfile

    nohup java -jar ${JVM_ARGS} -Dspring.profile.active=${PROFILE} ${PACKAGE_NAME} >/dev/null 2>&1 &

    echo "${PACKAGE_NAME} start success"
    return $?
}

stop(){
    checkPid
    if [ "${pid}"x != "0"x ];then
        echo "server is runing, kill process ${pid}"
        kill -9 "${pid}"
        rm -fr app.pid
        return 0
    fi
    echo "pid not exist check java process"
    checkJava
    if [ "${pid}"x != "0"x ];then
        echo "server is runing, kill process ${pid}"
        kill -9 "${pid}"
        rm -fr app.pid
        return 0
    fi
    echo "no process is running, nothing need stop"
    return 0
}

restart(){
    checkProfile
    stop
    start
    exit $?
}

checkProfile {
    PROFILE="dev"
}

checkJava(){
    v_pid=$(pgrep -f "${PACKAGE_NAME}" |head -l)
    if [ "${v_pid}"x = x ];then
        echo "java process is shutdown"
        pid=0
    else
        echo "java process is running pid:${v_pid}"
        echo "${v_pid}" > app.id
        pid=$((v_pid))
    fi
}

checkPid(){
    if [ -f "app.pid" ];then
        v_pid=$(head -1 app.id)
        if [ "${v_pid}"x = "0"x ];then
            pid=0
        else
            ps -ax | awk '${ print $1}' | grep -e "^${v_pid}$" > /dev/null

            if [ "$?" = "0" ];then
                echo "pid<${v_pid}> process is running"
                pid=$((v_pid))
            else
                echo "pid<${v_pid}> is not running"
                echo "clean app.id"
                rm -fr app.id
                pid=0
            fi
        fi
    else
        pid=0
    fi
}