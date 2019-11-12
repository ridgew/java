using System;
using System.Collections.Generic;
using System.Text;

namespace LoadBalance
{
    /**
 * Created by caojun on 2018/2/20.
 * https://www.cnblogs.com/markcd/p/8456870.html
 * https://github.com/yaozou/java/blob/master/src/main/java/com/yaozou/loadBalance/RoundRobinByWeightLoadBalance.java
 * 基本概念：
 * weight: 配置文件中指定的该后端的权重，这个值是固定不变的。
 * effective_weight: 后端的有效权重，初始值为weight。
 * 在释放后端时，如果发现和后端的通信过程中发生了错误，就减小effective_weight。
 * 此后有新的请求过来时，在选取后端的过程中，再逐步增加effective_weight，最终又恢复到weight。
 * 之所以增加这个字段，是为了当后端发生错误时，降低其权重。
 * current_weight:
 * 后端目前的权重，一开始为0，之后会动态调整。那么是怎么个动态调整呢？
 * 每次选取后端时，会遍历集群中所有后端，对于每个后端，让它的current_weight增加它的effective_weight，
 * 同时累加所有后端的effective_weight，保存为total。
 * 如果该后端的current_weight是最大的，就选定这个后端，然后把它的current_weight减去total。
 * 如果该后端没有被选定，那么current_weight不用减小。
 *
 * 算法逻辑：
 * 1. 对于每个请求，遍历集群中的所有可用后端，对于每个后端peer执行：
 *     peer->current_weight += peer->effecitve_weight。
 *     同时累加所有peer的effective_weight，保存为total。
 * 2. 从集群中选出current_weight最大的peer，作为本次选定的后端。
 * 3. 对于本次选定的后端，执行：peer->current_weight -= total。
 *
 */
    public class RoundRobinByWeightLoadBalance
    {
        //约定的invoker和权重的键值对
        private List<Node> nodes;

        public RoundRobinByWeightLoadBalance(Dictionary<IInvoker, int> invokersWeight)
        {
            if (invokersWeight != null && invokersWeight.Count > 0)
            {
                nodes = new List<Node>(invokersWeight.Count);
                foreach (var key in invokersWeight.Keys)
                {
                    nodes.Add(new Node(key, invokersWeight[key]));
                }
            }
        }

        /**
         * 算法逻辑：
         * 1. 对于每个请求，遍历集群中的所有可用后端，对于每个后端peer执行：
         *     peer->current_weight += peer->effecitve_weight。
         *     同时累加所有peer的effective_weight，保存为total。
         * 2. 从集群中选出current_weight最大的peer，作为本次选定的后端。
         * 3. 对于本次选定的后端，执行：peer->current_weight -= total。
         *
         * @Return ivoker
         */
        public IInvoker select()
        {
            if (!checkNodes())
            {
                return null;
            }
            else if (nodes.Count == 1)
            {
                if (nodes[0].invoker.isAvalable())
                {
                    return nodes[0].invoker;
                }
                else
                {
                    return null;
                }
            }

            int total = 0;
            Node nodeOfMaxWeight = null;

            nodes.ForEach(node =>
            {
                total += node.effectiveWeight;
                node.currentWeight += node.effectiveWeight;

                if (nodeOfMaxWeight == null)
                {
                    nodeOfMaxWeight = node;
                }
                else
                {
                    nodeOfMaxWeight = nodeOfMaxWeight.CompareTo(node) > 0 ? nodeOfMaxWeight : node;
                }
            });

            nodeOfMaxWeight.currentWeight -= total;
            return nodeOfMaxWeight.invoker;
        }

        private bool checkNodes()
        {
            return (nodes != null && nodes.Count > 0);
        }

        public void onInvokeSuccess(IInvoker invoker)
        {
            if (checkNodes())
            {
                int nodeIdx = nodes.FindIndex(n => invoker.id().Equals(n.invoker.id()));
                if (nodeIdx != -1)
                    nodes[nodeIdx].onInvokeSuccess();
            }
        }

        public void onInvokeFail(IInvoker invoker)
        {
            if (checkNodes())
            {
                int nodeIdx = nodes.FindIndex(n => invoker.id().Equals(n.invoker.id()));
                if (nodeIdx != -1)
                    nodes[nodeIdx].onInvokeFail();
            }
        }


        public void printCurrenctWeightBeforeSelect()
        {
            if (checkNodes())
            {
                StringBuilder sOut = new StringBuilder("{");
                nodes.ForEach(node =>
                {
                    sOut.Append(node.invoker.id())
                    .Append("=")
                    .Append(node.currentWeight + node.effectiveWeight)
                    .Append(",");
                });
                sOut.Append("}");

                Console.Write(sOut.ToString());
            }
        }

        public void printCurrenctWeight()
        {
            if (checkNodes())
            {
                StringBuilder sOut = new StringBuilder("{");
                nodes.ForEach(node =>
                {
                    sOut.Append(node.invoker.id())
                    .Append("=")
                    .Append(node.currentWeight)
                    .Append(",");
                });
                sOut.Append("}");

                Console.Write(sOut.ToString());
            }
        }

        public interface IInvoker
        {
            Boolean isAvalable();
            String id();
        }

        public class Invoker : IInvoker
        {
            public Invoker(string id)
            {
                ivkId = id;
            }

            string ivkId = null;

            public string id()
            {
                return ivkId;
            }

            public bool isAvalable()
            {
                return true;
            }
        }

        class Node : IComparable<Node>
        {
            internal IInvoker invoker;
            internal int weight;
            internal int effectiveWeight;
            internal int currentWeight;

            public Node(IInvoker invoker, int weight)
            {
                this.invoker = invoker;
                this.weight = weight;
                this.effectiveWeight = weight;
                this.currentWeight = 0;
            }

            public int CompareTo(Node o)
            {
                return currentWeight > o.currentWeight ? 1 : (currentWeight.Equals(o.currentWeight) ? 0 : -1);
            }

            public void onInvokeSuccess()
            {
                if (effectiveWeight < this.weight)
                {
                    effectiveWeight++;
                }
            }

            public void onInvokeFail()
            {
                effectiveWeight--;
            }

        }

        /// <summary>
        /// 静态测试入口
        /// </summary>
        public static void Main(string[] args)
        {
            Dictionary<IInvoker, int> invokersWeight = new Dictionary<IInvoker, int>(3);
            int aWeight = 410;
            int bWeight = 1841;
            int cWeight = 690;

            invokersWeight.Add(new Invoker("a"), aWeight);
            invokersWeight.Add(new Invoker("b"), bWeight);
            invokersWeight.Add(new Invoker("c"), cWeight);

            int times = 10_000;
            RoundRobinByWeightLoadBalance roundRobin = new RoundRobinByWeightLoadBalance(invokersWeight);
            for (int i = 1; i <= times; i++)
            {
                Console.Write(new StringBuilder(i + "").Append("    "));
                roundRobin.printCurrenctWeightBeforeSelect();
                IInvoker invoker = roundRobin.select();
                Console.Write(new StringBuilder("    ").Append(invoker.id()).Append("    "));
                roundRobin.printCurrenctWeight();
                Console.WriteLine();
            }
        }


    }
}
