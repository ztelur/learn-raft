// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;
use std::fmt::{Display, Formatter};

/// The state of the progress.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ProgressState {
    /// Whether it's probing.
    /// 当某个副本处于 Probe 状态时，Leader 只能给它发送 1 条 MsgAppend 消息
    /// 在这个状态下的 Progress 的 next_idx是 Leader 猜出来的，而不是由这个副本明确的上报信息推算出来的。
    /// 它有很大的概率是错误的，亦即 Leader 很可能会回退到某个地方重新发送；甚至有可能这个副本是不活跃的，那么 Leader 发送的整个滑动窗口的消息都可能浪费掉
    Probe,
    /// Whether it's replicating.
    /// Replicate 状态是最容易理解的，Leader 可以给对应的副本发送多个 MsgAppend 消息（不超过滑动窗口的限制），并适时地将窗口向前滑动
    Replicate,
    /// Whether it's a snapshot.
    /// 一个副本对应的 Progress 一旦处于这个状态，Leader 便不会再给这个副本发送任何 MsgAppend 了
    /// 事实上在 Leader 收到 MsgHeartbeatResponse 时，也会调用 Progress::resume来将取消对该副本的暂停，
    /// 然而对于 ProgressState::Snapshot 状态的 Progress 则没有这个逻辑。
    /// 这个状态会在 Leader 成功发送完成 Snapshot，或者收到了对应的副本的最新的 MsgAppendResponse 之后被改变
    Snapshot,
}

impl Default for ProgressState {
    fn default() -> ProgressState {
        ProgressState::Probe
    }
}

impl Display for ProgressState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ProgressState::Probe => write!(f, "StateProbe"),
            ProgressState::Replicate => write!(f, "StateReplicate"),
            ProgressState::Snapshot => write!(f, "StateSnapshot"),
        }
    }
}
