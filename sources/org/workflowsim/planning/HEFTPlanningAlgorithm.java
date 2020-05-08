/**
 * Copyright 2012-2013 University Of Southern California
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.workflowsim.planning;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.Log;
import org.workflowsim.CondorVM;
import org.workflowsim.FileItem;
import org.workflowsim.Task;
import org.workflowsim.utils.Parameters;

/**
 * The HEFT planning algorithm.
 *
 * @author Pedro Paulo Vezzá Campos
 * @date Oct 12, 2013
 */
public class HEFTPlanningAlgorithm extends BasePlanningAlgorithm {

    private Map<Task, Map<CondorVM, Double>> computationCosts;          // map<任务,map<虚拟机,任务在虚拟机上的计算成本>>  计算成本
    private Map<Task, Map<Task, Double>> transferCosts;
    private Map<Task, Double> rank;
    private Map<CondorVM, List<Event>> schedules;
    private Map<Task, Double> earliestFinishTimes;
    private double averageBandwidth;

    private class Event {        //内部类 事件类

        public double start;     //开始
        public double finish;    //完成

        public Event(double start, double finish) {
            this.start = start;
            this.finish = finish;
        }
    }

    private class TaskRank implements Comparable<TaskRank> {    //内部类，任务排名类

        public Task task;     //任务
        public Double rank;   //排名

        public TaskRank(Task task, Double rank) {
            this.task = task;
            this.rank = rank;
        }

        @Override
        public int compareTo(TaskRank o) {
            return o.rank.compareTo(rank);  //从大到小 排列
        }


    }

    public HEFTPlanningAlgorithm() {
        computationCosts = new HashMap<>();
        transferCosts = new HashMap<>();
        rank = new HashMap<>();
        earliestFinishTimes = new HashMap<>();
        schedules = new HashMap<>();
    }

    /**
     * The main function
     */
    @Override
    public void run() {
        Log.printLine("HEFT planner running with " + getTaskList().size()
                + " tasks.");

        averageBandwidth = calculateAverageBandwidth();   //虚拟机的可用平均带宽

        for (Object vmObject : getVmList()) {
            CondorVM vm = (CondorVM) vmObject;
            schedules.put(vm, new ArrayList<>());
        }

        // Prioritization phase
        calculateComputationCosts();
        calculateTransferCosts();
        calculateRanks();

        // Selection phase
        allocateTasks();
    }

    /**
     * Calculates the average available bandwidth among all VMs in Mbit/s
     * 计算所有虚拟机之间的平均可用带宽（Mbit/s）
     * @return Average available bandwidth in Mbit/s
     */
    private double calculateAverageBandwidth() {
        double avg = 0.0;
        for (Object vmObject : getVmList()) {
            CondorVM vm = (CondorVM) vmObject;
            avg += vm.getBw();
        }
        return avg / getVmList().size();
    }

    /**
     * Populates the computationCosts field with the time in seconds to compute
     * a task in a vm.
     * 使用在虚拟机中计算任务的时间（秒）填充“计算成本”字段。
     * 使用任务在虚拟机中的计算事件（秒）来填充“计算成本”字段。
     */
    private void calculateComputationCosts() {
        for (Task task : getTaskList()) {  //遍历所有任务
            Map<CondorVM, Double> costsVm = new HashMap<>();   //创建 该任务在所有  虚拟机-计算成本映射 虚拟机上的计算成本map<虚拟机对象，计算成本>
            for (Object vmObject : getVmList()) {
                CondorVM vm = (CondorVM) vmObject;
                if (vm.getNumberOfPes() < task.getNumberOfPes()) { //判断虚拟机的PE数量是否满足 任务需求的PE数量
                    costsVm.put(vm, Double.MAX_VALUE);  //不满足,将该任务在该虚拟机上的计算成本设为无穷大
                } else {   //满足,计算出"计算成本"，并存入 该任务在所有虚拟机上的计算成本map<虚拟机对象，计算成本>中
                    costsVm.put(vm,
                            task.getCloudletTotalLength() / vm.getMips());
                }
            }
            computationCosts.put(task, costsVm); //任务-(虚拟机计算成本映射) 映射
        }
    }

    /**
     * Populates the transferCosts map with the time in seconds to transfer all
     * files from each parent to each child
     * 在transferCosts映射中填充将所有文件从每个父级传输到每个子级的时间（秒）
     */
    private void calculateTransferCosts() {
        // Initializing the matrix
        for (Task task1 : getTaskList()) {  //task1,父任务
            Map<Task, Double> taskTransferCosts = new HashMap<>();   //创建 该父任务下的 子任务-传输成本 映射
            for (Task task2 : getTaskList()) {  //子任务
                taskTransferCosts.put(task2, 0.0);        //初始化 该父任务下的 子任务-传输成本 映射 ，传输成本初始值为0.0s ；
            }
            transferCosts.put(task1, taskTransferCosts);  // 初始化 任务传输成本 映射  父任务-(子任务-传输成本)
        }

        // Calculating the actual values
        for (Task parent : getTaskList()) {   //获取父任务
            for (Task child : parent.getChildList()) {   //获取子任务
                transferCosts.get(parent).put(child, calculateTransferCost(parent, child)); //给任务传输成本 映射  父任务-(子任务-传输成本) 的传输成本赋值
            }
        }
    }

    /**
     * Accounts the time in seconds necessary to transfer all files described
     *      * between parent and child
     *  计算父任务-子任务的传输成本
     * @param parent  父任务
     * @param child   子任务
     * @return Transfer cost in seconds 父任务-子任务的传输成本
     */
    private double calculateTransferCost(Task parent, Task child) {
        List<FileItem> parentFiles = parent.getFileList();  //获取父任务的文件列表
        List<FileItem> childFiles = child.getFileList();    //获取子任务的问件列表

        double acc = 0.0;

        for (FileItem parentFile : parentFiles) {  //遍历父任务的文件列表
            if (parentFile.getType() != Parameters.FileType.OUTPUT) {  //不是输出类型的文件 跳过
                continue;
            }

            for (FileItem childFile : childFiles) {  //遍历子任务的文件列表
                if (childFile.getType() == Parameters.FileType.INPUT
                        && childFile.getName().equals(parentFile.getName())) { //子任务的文件时输入类型，并且和父任务的文件名相同
                    acc += childFile.getSize();  //累计计算 当前父任务到当前子任务 需要传输的所有文件大小
                    break;
                }
            }
        }

        //file Size is in Bytes, acc in MB  换算单位，将bytes换算成为MB
        acc = acc / Consts.MILLION;
        // acc in MB, averageBandwidth in Mb/s
        return acc * 8 / averageBandwidth;    //使用 传输文件/平均带宽 计算文件传输成本
    }

    /**
     * Invokes calculateRank for each task to be scheduled
     * 为要调度的每个任务调用计算排名
     */
    private void calculateRanks() {
        for (Task task : getTaskList()) {
            calculateRank(task);
        }
    }

    /**
     * Populates rank.get(task) with the rank of task as defined in the HEFT
     * paper.
     * 填充rank.get（task）和HEFT paper中定义的task的等级。
     * @param task The task have the rank calculates
     * @return The rank
     */
    private double calculateRank(Task task) {  //计算任务排名
        if (rank.containsKey(task)) {  //如果当前任务排名已经存在，直接返回排名
            return rank.get(task);
        }

        double averageComputationCost = 0.0;  // 平均计算成本

        for (Double cost : computationCosts.get(task).values()) {   //遍历该任务在所有虚拟机上的计算成本
            averageComputationCost += cost;       // 该任务在所有虚拟机上的计算成本总和
        }

        averageComputationCost /= computationCosts.get(task).size();  //该任务的 平均计算成本  = 该任务在所有虚拟机上的计算成本总和/虚拟机个数

        double max = 0.0;
        for (Task child : task.getChildList()) {   //遍历当前任务的子任务列表
            double childCost = transferCosts.get(task).get(child)
                    + calculateRank(child);  //子任务成本 = 当前任务的子任务传输成本 + 子任务的任务排名
            max = Math.max(max, childCost);  //选出子任务成本的最高值
        }

        rank.put(task, averageComputationCost + max);  //父任务(排名) = 父任务的平均计算成本 + 子任务成本的最高值(父任务给子任务的传输成本 + 子任务的任务排名))

        return rank.get(task);
    }

    /**
     * Allocates all tasks to be scheduled in non-ascending order of schedule.
     */
    private void allocateTasks() {
        List<TaskRank> taskRank = new ArrayList<>();
        for (Task task : rank.keySet()) {
            taskRank.add(new TaskRank(task, rank.get(task)));
        }

        // Sorting in non-ascending order of rank
        Collections.sort(taskRank);  //根据排名降序排列
        for (TaskRank rank : taskRank) {
            allocateTask(rank.task);
        }

    }

    /**
     * Schedules the task given in one of the VMs minimizing the earliest finish
     * time
     * 将其中一个vm中给定的任务安排为最小化最早完成时间
     * @param task The task to be scheduled
     * @pre All parent tasks are already scheduled
     */
    private void allocateTask(Task task) {
        CondorVM chosenVM = null;
        double earliestFinishTime = Double.MAX_VALUE;  //将最早完成时间设为无穷大
        double bestReadyTime = 0.0;
        double finishTime;

        for (Object vmObject : getVmList()) {
            CondorVM vm = (CondorVM) vmObject;
            double minReadyTime = 0.0;                //最小准备时间

            for (Task parent : task.getParentList()) {
                double readyTime = earliestFinishTimes.get(parent);           //子任务和父任务的虚拟机号时：子任务的准备时间 = 父任务的最小完成时间
                if (parent.getVmId() != vm.getId()) {    //如果子任务和父任务的虚拟机号不同则 子任务的准备时间 = 父任务的最小完成时间 + 父任务到子任务的传输时间
                    readyTime += transferCosts.get(parent).get(task);
                }
                minReadyTime = Math.max(minReadyTime, readyTime);//子任务的最小准备时间 = 子任务玩的准备时间的最大值
            }

            finishTime = findFinishTime(task, vm, minReadyTime, false);  //计算完成时间

            if (finishTime < earliestFinishTime) {          //如果 任务在当前虚拟机上的完成时间 < 最早完成时间
                bestReadyTime = minReadyTime;               //则最好准备时间 = 任务在当前虚拟机上的最小准备时间
                earliestFinishTime = finishTime;            //最早完成时间 = 任务在当前虚拟机的完成时间
                chosenVM = vm;                              //选择虚拟机 = 当前虚拟机
            }
        }

        findFinishTime(task, chosenVM, bestReadyTime, true);
        earliestFinishTimes.put(task, earliestFinishTime);

        task.setVmId(chosenVM.getId());
    }

    /**
     * Finds the best time slot available to minimize the finish time of the
     * given task in the vm with the constraint of not scheduling it before
     * readyTime. If occupySlot is true, reserves the time slot in the schedule.
     * 查找可用的最佳时隙，以最小化vm中给定任务的完成时间，并限制在重新分析之前不调度该任务。
     * 如果occupySlot为true，则在计划中保留时间段。
     * @param task The task to have the time slot reserved
     * @param vm The vm that will execute the task
     * @param readyTime The first moment that the task is available to be
     * scheduled
     * @param occupySlot If true, reserves the time slot in the schedule.
     * @return The minimal finish time of the task in the vmn
     */
    private double findFinishTime(Task task, CondorVM vm, double readyTime,
                                  boolean occupySlot) {
        List<Event> sched = schedules.get(vm);
        double computationCost = computationCosts.get(task).get(vm);  //获取此任务在此虚拟机上的计算成本（花费时间）
        double start, finish;
        int pos;

        if (sched.isEmpty()) {
            if (occupySlot) {
                sched.add(new Event(readyTime, readyTime + computationCost));
            }
            return readyTime + computationCost;
        }

        if (sched.size() == 1) {
            if (readyTime >= sched.get(0).finish) {
                pos = 1;
                start = readyTime;
            } else if (readyTime + computationCost <= sched.get(0).start) {
                pos = 0;
                start = readyTime;
            } else {
                pos = 1;
                start = sched.get(0).finish;
            }

            if (occupySlot) {
                sched.add(pos, new Event(start, start + computationCost));
            }
            return start + computationCost;
        }

        // Trivial case: Start after the latest task scheduled
        start = Math.max(readyTime, sched.get(sched.size() - 1).finish);
        finish = start + computationCost;
        int i = sched.size() - 1;
        int j = sched.size() - 2;
        pos = i + 1;
        while (j >= 0) {
            Event current = sched.get(i);
            Event previous = sched.get(j);

            if (readyTime > previous.finish) {
                if (readyTime + computationCost <= current.start) {
                    start = readyTime;
                    finish = readyTime + computationCost;
                }

                break;
            }
            if (previous.finish + computationCost <= current.start) {
                start = previous.finish;
                finish = previous.finish + computationCost;
                pos = i;
            }
            i--;
            j--;
        }

        if (readyTime + computationCost <= sched.get(0).start) {
            pos = 0;
            start = readyTime;

            if (occupySlot) {
                sched.add(pos, new Event(start, start + computationCost));
            }
            return start + computationCost;
        }
        if (occupySlot) {
            sched.add(pos, new Event(start, finish));
        }
        return finish;
    }
}
