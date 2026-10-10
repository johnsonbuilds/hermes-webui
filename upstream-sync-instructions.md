# Upstream Sync Instructions (上游同步指南)

本文件既是当前 fork 相对 upstream（nesquena/hermes-webui）的差异清单，
也是后续同步上游代码时的操作指南。照抄 hermes-agent fork 的同名文件约定的格式。

## 核心原则

- 下文"差异点"章节中的"新增文件"和"修改文件"共同构成需要保留的 fork 差异点清单。
- 后续同步 upstream 最新代码时，必须显式核对并保留差异点，避免在冲突解决、
  批量覆盖或清理过程中误删本 fork 的既有定制行为。
- 如果后续本 fork 引入了新的差异点，必须持续准确记录到本文件。
- `.github/workflows/docker-publish-clawcloud.yml` 是 fork 专属的发布流水线，
  上游没有同名文件，同步时永远保留。

## 当前差异点 (Differences)

### 新增文件 (New Files)

- `api/runtime_panel.py`: Runtime Panel 后端——把原始 SSE 事件流
  （token、tool、tool_complete、done 等）转成适合展示的简化时间线。
  详见 `tests/test_runtime_panel.py` 的行为约定。

- `tests/test_runtime_panel.py`: Runtime Panel 的行为测试。

- `.github/workflows/docker-publish-clawcloud.yml`: fork 镜像发布流水线
  （amd64 + arm64 原生构建，digest-merge 多架构 manifest，`/health` 冒烟）。
  上游 `release.yml` 只在 tag 时发布官方镜像，不覆盖 fork 的按分支构建需求。

### 修改文件 (Modified Files)

- `api/routes.py`: 注册 Runtime Panel 的路由（约 49 行，纯新增路由分支，
  不改动现有 if/elif 顺序——合并时注意上游路由表新增项的位置）。

- `api/config.py`: Runtime Panel 的配置接线（约 8 行）。

- `static/index.html`: Runtime Panel 入口挂载（约 26 行）。

- `static/panels.js`: Runtime Panel 前端面板（约 397 行）。

- `static/panels.js` / `static/style.css` / `static/i18n.js`: 面板样式与文案。

### 已丢弃的不再保留项 (Dropped)

以下曾存在于旧 `origin/runtime-panel` 分支（基线 2026-07-18）的定制，
在 2026-10-08 迁移到当前 master 时确认不再需要，后续同步无需恢复：

- OpenCode Go 模型目录补全：上游已独立同步了更新的目录
  （含 `glm-5.3-flash`、`muse-spark-1.3-contributor` 等），旧补丁整体过时。
- `fix: hide internal continuation prompts` 与
  `fix: avoid duplicate error-turn recovery messages` 及其两条 revert：
  两对 fix+revert 净效果为零，且上游已重构相关 `streaming.py` 区域。

## 同步操作步骤

1. `git fetch upstream`，在 `clawcloud` 分支上 `git cherry-pick` 或 `rebase`
   所需的上游更新；本文件的差异点逐个核对保留。
2. 跑 `./scripts/test.sh tests/test_runtime_panel.py -q`（主约定测试）
   加相邻路由/配置测试；上游全量门（含 ruff）按 `CONTRIBUTING.md` 执行。
3. 合到 `master` 并 push——`docker-publish-clawcloud.yml` 自动打出新镜像；
   同步更新 `hermes-cloud-image/versions.env` 的 `WEBUI_IMAGE` pin。
