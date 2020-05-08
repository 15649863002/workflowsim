/**
 * Copyright 2012-2013 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.workflowsim.scheduling;

import java.util.ArrayList;
import java.util.List;
import org.cloudbus.cloudsim.Cloudlet;
import org.workflowsim.CondorVM;
import org.workflowsim.WorkflowSimTags;

/**
 * MinMin algorithm.
 *
 * @author Weiwei Chen
 * @since WorkflowSim Toolkit 1.0
 * @date Apr 9, 2013
 */
public class MinMinSchedulingAlgorithm extends BaseSchedulingAlgorithm {

    public MinMinSchedulingAlgorithm() {
        super();
    }
    private final List<Boolean> hasChecked = new ArrayList<>();   //是否对比过

    @Override
    public void run() {

        int size = getCloudletList().size();            //就绪任务数量
        List list = getCloudletList();
        hasChecked.clear();                             //清除hasChecked
        for (int t = 0; t < size; t++) {                //初始化hasChecked 全部设为false
            hasChecked.add(false);
        }
        for (int i = 0; i < size; i++) {
            int minIndex = 0;                           //最小任务的下标
            Cloudlet minCloudlet = null;                //最小任务
            for (int j = 0; j < size; j++) {
                Cloudlet cloudlet = (Cloudlet) getCloudletList().get(j);     //遍历已就绪任务
                if (!hasChecked.get(j)) {                                    //判断状态 false 则进入
                    minCloudlet = cloudlet;                                  //当前任务赋值给最小任务
                    minIndex = j;                                            //记录当前任务下标，为最小任务的标
                    break;                                                   //跳出循环
                }
            }
            if (minCloudlet == null) {                  //如果最小任务为空 结束当前循环
                break;
            }

                                                        //最小任务不为空
            for (int j = 0; j < size; j++) {            //遍历当前已就绪任务
                Cloudlet cloudlet = (Cloudlet) getCloudletList().get(j);
                if (hasChecked.get(j)) {                //判断状态  true 进入
                    continue;                            //跳过本次循环
                }
                long length = cloudlet.getCloudletLength();        //获取当前任务的指令（MI）长度
                if (length < minCloudlet.getCloudletLength()) {    //当前任务是否小于最小任务 是则记录当前任务为最小任务
                    minCloudlet = cloudlet;
                    minIndex = j;
                }
            }
            hasChecked.set(minIndex, true);

            int vmSize = getVmList().size();
            CondorVM firstIdleVm = null;//(CondorVM)getVmList().get(0);     //执行当前任务速度最快的虚拟机
            for (int j = 0; j < vmSize; j++) {                              //遍历所有虚拟机
                CondorVM vm = (CondorVM) getVmList().get(j);
                if (vm.getState() == WorkflowSimTags.VM_STATUS_IDLE) {      //查看当前虚拟机是否可用
                    firstIdleVm = vm;                                       //将其标记为最快虚拟机
                    break;                                                  //跳出循环
                }
            }
            if (firstIdleVm == null) {               //未找到就绪虚拟机
                break;                               //跳出循环
            }
            for (int j = 0; j < vmSize; j++) {                               //遍历所有虚拟机
                CondorVM vm = (CondorVM) getVmList().get(j);
                if ((vm.getState() == WorkflowSimTags.VM_STATUS_IDLE)        //查看当前虚拟机是否可用
                        && vm.getCurrentRequestedTotalMips() > firstIdleVm.getCurrentRequestedTotalMips()) {   //比较当前虚拟是否比最快虚拟机还快（总带宽）
                    firstIdleVm = vm;
                }
            }
            firstIdleVm.setState(WorkflowSimTags.VM_STATUS_BUSY);           //将找到的最快虚拟机标记为繁忙 不可用
            minCloudlet.setVmId(firstIdleVm.getId());                       //将当前就绪任务中的最小任务，与当前最快的虚拟机绑定
            getScheduledList().add(minCloudlet);                            //将最小任务提交到调度列表中
        }
    }
}
