document.addEventListener('DOMContentLoaded', () => {
    const searchForm = document.getElementById('search-form');
    const searchInput = document.getElementById('search-query');
    const searchBtn = document.getElementById('search-btn');
    const pipelineSection = document.getElementById('pipeline-section');
    const resultsSection = document.getElementById('results-section');
    const logsContainer = document.getElementById('pipeline-logs');
    const highlightsContainer = document.getElementById('highlights-container');
    const sentimentInsights = document.getElementById('sentiment-insights');
    const recordCountLabel = document.getElementById('record-count');
    const tableHeaders = document.getElementById('table-headers');
    const tableBody = document.getElementById('table-body');
    const suggestTags = document.querySelectorAll('.suggest-tag');

    // Scheduler Elements
    const schedulerForm = document.getElementById('scheduler-form');
    const scheduleQueryInput = document.getElementById('schedule-query-input');
    const scheduleRateSelect = document.getElementById('schedule-rate-select');
    const schedulerStatus = document.getElementById('scheduler-status');
    const schedulesListContainer = document.getElementById('schedules-list');

    // Directory Elements
    const refreshDirBtn = document.getElementById('refresh-directory-btn');
    const datasetsGrid = document.getElementById('datasets-grid');

    // Custom Dropdown UI Handling
    const customDropdown = document.getElementById('custom-duration-dropdown');
    const customTrigger = document.getElementById('custom-dropdown-trigger');
    const customMenu = document.getElementById('custom-dropdown-menu');
    const customItems = document.querySelectorAll('.custom-dropdown-item');
    const customSelectedText = document.getElementById('custom-dropdown-selected-text');

    if (customTrigger && customMenu && customDropdown) {
        customTrigger.addEventListener('click', (e) => {
            e.stopPropagation();
            customMenu.classList.toggle('hidden');
            customDropdown.classList.toggle('open');
        });

        customItems.forEach(item => {
            item.addEventListener('click', (e) => {
                e.stopPropagation();
                const val = item.getAttribute('data-value');
                const text = item.textContent;

                customSelectedText.textContent = text;
                if (scheduleRateSelect) {
                    scheduleRateSelect.value = val;
                    scheduleRateSelect.dispatchEvent(new Event('change'));
                }

                customItems.forEach(i => i.classList.remove('active'));
                item.classList.add('active');

                customMenu.classList.add('hidden');
                customDropdown.classList.remove('open');
            });
        });

        document.addEventListener('click', () => {
            customMenu.classList.add('hidden');
            customDropdown.classList.remove('open');
        });
    }

    // State Variables
    let analyticsChart = null;
    let currentItems = [];
    let currentQuery = '';
    let eventBusSource = null;
    let pollingInterval = null;
    let pollingTimeout = null;

    // ----------------------------------------------------------------------
    // 1. Event Bus (SSE Connection) Setup
    // ----------------------------------------------------------------------
    function connectToEventBus() {
        console.log("Connecting to AWS Event Bus Simulator...");
        
        eventBusSource = new EventSource('/api/event-bus');

        eventBusSource.onopen = () => {
            console.log("AWS Event Bus Connection Opened.");
        };

        eventBusSource.onmessage = (event) => {
            const payload = JSON.parse(event.data);
            
            if (payload.event === 'connected') {
                console.log(payload.log);
                return;
            }

            // Handle system toasts (e.g. background schedule alerts)
            if (payload.event === 'toast') {
                showToast(payload.log, 'info');
                return;
            }

            const isCurrentQuery = currentQuery.toLowerCase() === payload.query.toLowerCase();

            if (payload.event === 'progress') {
                if (isCurrentQuery) {
                    // Show progress on current search
                    if (pipelineSection.classList.contains('hidden')) {
                        pipelineSection.classList.remove('hidden');
                        resultsSection.classList.add('hidden');
                    }
                    if (payload.log) {
                        addLog(payload.log, 'info');
                    }
                    if (payload.step) {
                        updatePipelineStep(payload.step, payload.status, payload.status_text);
                    }
                } else {
                    console.log(`[Background Queue] Scraper progress on topic '${payload.query}': ${payload.log}`);
                }
            }

            if (payload.event === 'completed') {
                // Clear any active polling fallback for this query
                if (isCurrentQuery) {
                    if (pollingInterval) {
                        clearInterval(pollingInterval);
                        pollingInterval = null;
                    }
                    if (pollingTimeout) {
                        clearTimeout(pollingTimeout);
                        pollingTimeout = null;
                    }
                }
                fetchDirectory(); // Refresh directory list
                if (isCurrentQuery) {
                    if (payload.log) addLog(payload.log, 'success');
                    if (payload.step) updatePipelineStep(payload.step, 'completed', 'Success');
                    
                    showToast(`Job search completed for "${payload.query}"!`, 'success');
                    
                    // Render final data
                    setTimeout(() => {
                        renderResults(payload.data);
                        searchBtn.disabled = false;
                        searchBtn.querySelector('span').textContent = 'Find Live Jobs';
                    }, 800);
                } else {
                    showToast(`EventBridge fetched new background feeds for "${payload.query}"!`, 'success');
                    console.log(`[EventBridge] Automated run for '${payload.query}' complete.`);
                }
            }
        };

        eventBusSource.onerror = (error) => {
            console.error('Event Bus connection error:', error);
            eventBusSource.close();
            
            showToast('Lost connection to AWS Simulator. Reconnecting in 3s...', 'error');
            setTimeout(connectToEventBus, 3000);
        };
    }

    // Connect immediately on load
    connectToEventBus();

    // ----------------------------------------------------------------------
    // 2. Search & Pipeline Management
    // ----------------------------------------------------------------------
    suggestTags.forEach(tag => {
        tag.addEventListener('click', () => {
            searchInput.value = tag.textContent;
            searchForm.dispatchEvent(new Event('submit'));
        });
    });

    searchForm.addEventListener('submit', (e) => {
        e.preventDefault();
        const query = searchInput.value.trim();
        if (!query) return;

        triggerSearch(query);
    });

    function triggerSearch(query) {
        currentQuery = query;
        resetPipelineUI();
        addLog(`Initiating AWS cloud job search for "${query}"`, 'info');

        // Call API Gateway trigger (Asynchronous POST enqueuing request to SQS)
        fetch(`/api/search?query=${encodeURIComponent(query)}`, {
            method: 'POST'
        })
        .then(response => {
            if (response.status === 200 || response.status === 202) {
                return response.json();
            } else {
                throw new Error('Server returned error status: ' + response.status);
            }
        })
        .then(data => {
            console.log(`Pipeline enqueued successfully for query: '${query}'`);
            addLog("Search request enqueued in AWS SQS queue. Processing pipeline asynchronously...", 'info');
            
            // Start the polling fallback in case SSE is slow or blocked
            startPollingFallback(query);
        })
        .catch(error => {
            console.error('Error triggering search:', error);
            showToast(`Search execution failed: ${error.message}`, 'error');
            
            // Clear loader
            searchBtn.disabled = false;
            searchBtn.querySelector('span').textContent = 'Find Live Jobs';
            
            addLog(`Error: ${error.message}. Pipeline stopped.`, 'error');
            setTimeout(() => {
                renderResults({ query: query, queryType: 'reviews', items: [] });
            }, 500);
        });
    }

    function startPollingFallback(query) {
        // Clear any existing timers
        if (pollingInterval) clearInterval(pollingInterval);
        if (pollingTimeout) clearTimeout(pollingTimeout);

        // Start polling after 4 seconds if SSE hasn't completed
        pollingTimeout = setTimeout(() => {
            if (currentQuery.toLowerCase() !== query.toLowerCase()) return;
            console.log(`SSE slow/inactive. Starting HTTP polling fallback for '${query}'...`);
            addLog("Job search polling safety net activated (Event Bus slow/inactive)...", 'info');
            
            pollingInterval = setInterval(() => {
                if (currentQuery.toLowerCase() !== query.toLowerCase()) {
                    clearInterval(pollingInterval);
                    return;
                }
                
                fetch(`/api/results?query=${encodeURIComponent(query)}`)
                .then(res => {
                    if (res.status === 200) return res.json();
                    throw new Error('Data not ready yet');
                })
                .then(data => {
                    console.log(`Polling fallback fetched results successfully for '${query}'`);
                    clearInterval(pollingInterval);
                    pollingInterval = null;
                    
                    if (currentQuery.toLowerCase() === query.toLowerCase()) {
                        addLog("Pipeline completed (detected via database poll).", 'success');
                        showToast(`Job search completed for "${query}"!`, 'success');
                        
                        setTimeout(() => {
                            renderResults(data);
                            searchBtn.disabled = false;
                            searchBtn.querySelector('span').textContent = 'Find Live Jobs';
                        }, 500);
                    }
                })
                .catch(err => {
                    // Result not ready yet, continue polling
                    console.log("Polling for result database entry...");
                });
            }, 2000);
        }, 4000);
    }

    function resetPipelineUI() {
        if (pollingInterval) {
            clearInterval(pollingInterval);
            pollingInterval = null;
        }
        if (pollingTimeout) {
            clearTimeout(pollingTimeout);
            pollingTimeout = null;
        }
        document.querySelectorAll('.pipeline-step').forEach(step => {
            step.className = 'pipeline-step';
            step.querySelector('.step-status').textContent = 'Pending';
        });
        document.querySelectorAll('.pipeline-connector').forEach(conn => {
            conn.className = 'pipeline-connector';
        });
        logsContainer.innerHTML = '';
        pipelineSection.classList.remove('hidden');
        resultsSection.classList.add('hidden');
        searchBtn.disabled = true;
        searchBtn.querySelector('span').textContent = 'Searching...';
    }

    function addLog(text, type = 'info') {
        const p = document.createElement('p');
        p.className = `log-entry ${type}`;
        const time = new Date().toLocaleTimeString();
        p.textContent = `[${time}] ${text}`;
        logsContainer.appendChild(p);
        logsContainer.scrollTop = logsContainer.scrollHeight;
    }

    function updatePipelineStep(stepId, status, statusText) {
        const stepEl = document.getElementById(`step-${stepId}`);
        if (!stepEl) return;

        stepEl.classList.remove('active', 'completed', 'failed');
        stepEl.classList.add(status);
        
        stepEl.querySelector('.step-status').textContent = statusText;

        const connector = stepEl.nextElementSibling;
        if (connector && connector.classList.contains('pipeline-connector')) {
            connector.classList.remove('active', 'completed');
            if (status === 'completed') {
                connector.classList.add('completed');
            } else if (status === 'active') {
                connector.classList.add('active');
            }
        }
    }

    function showToast(message, type = 'info') {
        const toastContainer = document.getElementById('toast-container');
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        
        let icon = 'fa-circle-info';
        if (type === 'success') icon = 'fa-circle-check';
        if (type === 'error') icon = 'fa-circle-xmark';

        toast.innerHTML = `
            <i class="fa-solid ${icon}"></i>
            <span>${message}</span>
        `;
        
        toastContainer.appendChild(toast);
        setTimeout(() => {
            toast.remove();
        }, 5000);
    }

    // ----------------------------------------------------------------------
    // 3. Render Widgets & Data Dashboard
    // ----------------------------------------------------------------------
    function renderResults(data) {
        currentItems = data.items || [];
        currentQuery = data.query || 'search';

        pipelineSection.classList.add('hidden');
        resultsSection.classList.remove('hidden');

        if (currentItems.length === 0) {
            highlightsContainer.innerHTML = '';
            tableHeaders.innerHTML = '';
            tableBody.innerHTML = '<tr><td style="text-align: center; color: var(--text-secondary); padding: 40px; font-size: 1.1rem;"><i class="fa-solid fa-circle-info" style="margin-right:8px;"></i> No live listings or reviews found. Please try a different query (e.g. "TCS reviews" or "Sales jobs").</td></tr>';
            recordCountLabel.textContent = '0 items found';
            if (analyticsChart) {
                analyticsChart.destroy();
                analyticsChart = null;
            }
            document.getElementById('pipeline-metrics').innerHTML = '<div style="color:var(--text-secondary); padding:10px;">No performance stats.</div>';
            sentimentInsights.innerHTML = '<div style="color:var(--text-secondary); padding:10px;">No sentiment insights.</div>';
            return;
        }

        // 1. Highlights
        highlightsContainer.innerHTML = '';
        if (data.highlights) {
            data.highlights.forEach(hl => {
                const card = document.createElement('div');
                card.className = 'highlight-card';
                card.innerHTML = `
                    <div class="highlight-icon ${hl.colorClass}">
                        <i class="fa-solid ${hl.icon}"></i>
                    </div>
                    <div class="highlight-content">
                        <h4>${hl.title}</h4>
                        <div class="highlight-value">${hl.value}</div>
                        <div class="highlight-desc">${hl.desc}</div>
                    </div>
                `;
                highlightsContainer.appendChild(card);
            });
        }

        // 2. Charts
        if (data.analytics) {
            renderCharts(data.analytics, data.queryType);
        }

        // 3. Sentiment
        if (data.sentiment) {
            renderSentiment(data.sentiment);
        }

        // 4. Table
        if (data.items) {
            renderTable(data.items);
        }

        // 5. Live Pipeline Metrics (Twist)
        renderPerformanceMetrics(data);

        // 6. Append Triggered Alerts if present
        if (data.alerts && data.alerts.length > 0) {
            appendAlertLogs(data.alerts);
        }
    }

    function renderCharts(analytics, queryType) {
        const ctx = document.getElementById('analytics-chart').getContext('2d');
        const metricLabel = document.getElementById('chart-metric-label');

        if (analyticsChart) {
            analyticsChart.destroy();
        }

        let chartConfig = {};

        if (queryType === 'jobs') {
            metricLabel.textContent = 'Salary Distribution (LPA)';
            chartConfig = {
                type: 'bar',
                data: {
                    labels: analytics.labels,
                    datasets: [
                        {
                            label: 'Number of Job Postings',
                            data: analytics.values,
                            backgroundColor: 'rgba(168, 85, 247, 0.65)',
                            borderColor: '#a855f7',
                            borderWidth: 1.5,
                            borderRadius: 6
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: {
                        y: {
                            grid: { color: 'rgba(255, 255, 255, 0.05)' },
                            ticks: { color: '#9ca3af', stepSize: 1 }
                        },
                        x: {
                            grid: { display: false },
                            ticks: { color: '#9ca3af' }
                        }
                    }
                }
            };
        } else {
            metricLabel.textContent = 'Ratings Distribution (Stars)';
            chartConfig = {
                type: 'bar',
                data: {
                    labels: analytics.labels,
                    datasets: [
                        {
                            label: 'Number of Reviews',
                            data: analytics.values,
                            backgroundColor: 'rgba(251, 191, 36, 0.65)',
                            borderColor: '#fbbf24',
                            borderWidth: 1.5,
                            borderRadius: 6
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: {
                        y: {
                            grid: { color: 'rgba(255, 255, 255, 0.05)' },
                            ticks: { color: '#9ca3af', stepSize: 1 }
                        },
                        x: {
                            grid: { display: false },
                            ticks: { color: '#9ca3af' }
                        }
                    }
                }
            };
        }

        analyticsChart = new Chart(ctx, chartConfig);
    }

    function renderSentiment(sentiment) {
        sentimentInsights.innerHTML = `
            <div class="sentiment-bar-wrapper">
                <div class="sentiment-label">
                    <span>Positive Sentiment Score</span>
                    <span>${sentiment.positive}%</span>
                </div>
                <div class="sentiment-bar-container">
                    <div class="sentiment-fill" style="width: ${sentiment.positive}%; background-color: var(--accent-emerald);"></div>
                </div>
            </div>
            <div class="sentiment-bar-wrapper">
                <div class="sentiment-label">
                    <span>Neutral / Standard</span>
                    <span>${sentiment.neutral}%</span>
                </div>
                <div class="sentiment-bar-container">
                    <div class="sentiment-fill" style="width: ${sentiment.neutral}%; background-color: var(--accent-blue);"></div>
                </div>
            </div>
            <div class="sentiment-bar-wrapper">
                <div class="sentiment-label">
                    <span>Negative Sentiment (Alerts/Reviews)</span>
                    <span>${sentiment.negative}%</span>
                </div>
                <div class="sentiment-bar-container">
                    <div class="sentiment-fill" style="width: ${sentiment.negative}%; background-color: var(--accent-rose);"></div>
                </div>
            </div>
            <div class="sentiment-insights-text">
                <strong>AWS Comprehend Extraction:</strong><br>
                ${sentiment.keywords.map(kw => `<span class="insight-pill">${kw}</span>`).join(' ')}
                <p style="margin-top:10px; font-style:italic;">"${sentiment.summary}"</p>
            </div>
        `;
    }

    function renderTable(items) {
        recordCountLabel.textContent = `${items.length} items loaded`;
        tableHeaders.innerHTML = '';
        tableBody.innerHTML = '';

        if (items.length === 0) return;

        const firstItem = items[0];
        const keys = Object.keys(firstItem).filter(key => key !== 'id' && key !== 'ItemId' && key !== 'SearchQuery');

        keys.forEach(key => {
            const th = document.createElement('th');
            let displayHeader = key.charAt(0).toUpperCase() + key.slice(1);
            if (key === 'workload_rating') displayHeader = 'Work-Life Rating';
            if (key === 'salary_rating') displayHeader = 'Salary Rating';
            th.textContent = displayHeader;
            tableHeaders.appendChild(th);
        });

        items.forEach(item => {
            const tr = document.createElement('tr');
            keys.forEach(key => {
                const td = document.createElement('td');
                const val = item[key];

                if (key.toLowerCase() === 'link' || key.toLowerCase() === 'url') {
                    td.innerHTML = `<a href="${val}" target="_blank" class="table-link-btn" style="color: #6366f1; font-weight: 600; text-decoration: none; display: inline-flex; align-items: center; gap: 4px;"><i class="fa-solid fa-arrow-up-right-from-square"></i> Open Source</a>`;
                } else if (key.toLowerCase() === 'rating' || key.toLowerCase() === 'workload_rating' || key.toLowerCase() === 'salary_rating') {
                    td.innerHTML = `<span class="rating-badge" style="background: rgba(251, 191, 36, 0.1); color: #fbbf24; border: 1px solid rgba(251, 191, 36, 0.2); padding: 2px 6px; border-radius: 4px; font-size: 0.8rem; display: inline-flex; align-items: center; gap: 4px;"><i class="fa-solid fa-star"></i> ${val}</span>`;
                } else if (key.toLowerCase() === 'price' || key.toLowerCase() === 'salary') {
                    td.style.fontWeight = '600';
                    td.textContent = val;
                } else {
                    td.textContent = val;
                }
                tr.appendChild(td);
            });
            tableBody.appendChild(tr);
        });
    }

    // ----------------------------------------------------------------------
    // 4. Amazon EventBridge Scheduler Interface Logic
    // ----------------------------------------------------------------------
    schedulerForm.addEventListener('submit', (e) => {
        e.preventDefault();
        const query = scheduleQueryInput.value.trim();
        const rate = scheduleRateSelect.value;
        const emailInput = document.getElementById('schedule-email-input');
        const email = emailInput ? emailInput.value.trim() : '';
        if (!query) return;

        createSchedule(query, rate, email);
    });

    function createSchedule(query, rateSeconds, email) {
        fetch(`/api/schedule?query=${encodeURIComponent(query)}&rate=${rateSeconds}&email=${encodeURIComponent(email)}`, {
            method: 'POST'
        })
        .then(response => {
            if (response.ok) {
                scheduleQueryInput.value = '';
                const emailInput = document.getElementById('schedule-email-input');
                if (emailInput) emailInput.value = '';
                fetchSchedules(); // Refresh active list
            }
        })
        .catch(err => {
            console.error('Error scheduling scraper:', err);
            showToast('Unable to reach simulator to register EventBridge schedule.', 'error');
        });
    }

    function fetchSchedules() {
        fetch('/api/schedules')
        .then(res => res.json())
        .then(data => {
            schedulerStatus.textContent = `Active Feeds: ${data.length}`;
            schedulesListContainer.innerHTML = '';

            if (data.length === 0) {
                schedulesListContainer.innerHTML = '<p class="no-schedules-msg">No active automated triggers. Create one above!</p>';
                return;
            }

            data.forEach(sch => {
                const item = document.createElement('div');
                item.className = 'schedule-item';
                
                let rateLabel = `Every ${sch.interval} seconds`;
                if (sch.interval === 86400) rateLabel = 'Daily (0 0 * * ?)';
                if (sch.interval === 604800) rateLabel = 'Weekly (0 0 ? * SUN)';

                const emailLabel = sch.email ? `<span class="email-badge" style="background: rgba(99, 102, 241, 0.15); color: #818cf8; border: 1px solid rgba(99, 102, 241, 0.25); padding: 2px 6px; border-radius: 4px; font-size: 0.8rem; display: inline-flex; align-items: center; gap: 4px; margin-left: 8px;"><i class="fa-solid fa-envelope"></i> ${sch.email}</span>` : '';

                item.innerHTML = `
                    <div class="schedule-info">
                        <span class="topic-badge">${sch.query}</span>
                        <span class="rate-text"><i class="fa-solid fa-clock"></i> ${rateLabel}</span>
                        ${emailLabel}
                    </div>
                    <button class="delete-schedule-btn" data-query="${sch.query}" title="Delete Schedule">
                        <i class="fa-solid fa-trash-can"></i>
                    </button>
                `;
                
                // Hook up delete listener
                item.querySelector('.delete-schedule-btn').addEventListener('click', (e) => {
                    const q = e.currentTarget.getAttribute('data-query');
                    deleteSchedule(q);
                });

                schedulesListContainer.appendChild(item);
            });
        })
        .catch(err => console.error('Error fetching schedules:', err));
    }

    function deleteSchedule(query) {
        fetch(`/api/unschedule?query=${encodeURIComponent(query)}`, {
            method: 'POST'
        })
        .then(res => {
            if (res.ok) {
                fetchSchedules();
            }
        });
    }

    // Load active schedules and datasets directory on startup
    fetchSchedules();
    fetchDirectory();

    refreshDirBtn.addEventListener('click', fetchDirectory);

    function fetchDirectory() {
        fetch('/api/queries')
        .then(res => res.json())
        .then(data => {
            datasetsGrid.innerHTML = '';
            if (data.length === 0) {
                datasetsGrid.innerHTML = '<p class="no-datasets-msg">No history found in database. Search above or configure a background feed!</p>';
                return;
            }
            
            data.forEach(db => {
                const card = document.createElement('div');
                card.className = 'dataset-card';
                
                card.innerHTML = `
                    <div class="dataset-info">
                        <span class="dataset-name">${db.query}</span>
                        <span class="dataset-meta">${db.items_count} listings • Saved: ${db.scraped_at}</span>
                    </div>
                    <button class="load-dataset-btn" data-query="${db.query}">
                        <i class="fa-solid fa-cloud-arrow-down"></i> Load Report
                    </button>
                `;
                
                card.querySelector('.load-dataset-btn').addEventListener('click', (e) => {
                    const q = e.currentTarget.getAttribute('data-query');
                    loadDataset(q);
                });
                
                datasetsGrid.appendChild(card);
            });
        })
        .catch(err => console.error('Error fetching directory:', err));
    }

    function loadDataset(query) {
        showToast(`Loading query "${query}" from DynamoDB...`, 'info');
        fetch(`/api/results?query=${encodeURIComponent(query)}`)
        .then(res => res.json())
        .then(data => {
            renderResults(data);
            searchInput.value = query;
            showToast(`Loaded dataset for "${query}"!`, 'success');
        })
        .catch(err => {
            console.error('Error loading dataset:', err);
            showToast('Failed to fetch dataset from database.', 'error');
        });
    }

    // ----------------------------------------------------------------------
    // 5. CSV / JSON Table Exports
    // ----------------------------------------------------------------------
    const exportCsvBtn = document.getElementById('export-csv-btn');
    const exportJsonBtn = document.getElementById('export-json-btn');

    exportCsvBtn.addEventListener('click', () => {
        if (!currentItems || currentItems.length === 0) {
            showToast('No records available to export.', 'error');
            return;
        }
        
        const keys = Object.keys(currentItems[0]).filter(k => k !== 'ItemId' && k !== 'SearchQuery');
        let csvContent = keys.join(',') + '\n';
        
        currentItems.forEach(item => {
            let row = keys.map(k => {
                let val = item[k] || '';
                val = val.toString().replace(/"/g, '""');
                return `"${val}"`;
            });
            csvContent += row.join(',') + '\n';
        });
        
        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.setAttribute('href', url);
        link.setAttribute('download', `scraped_results_${currentQuery.replace(/\s+/g, '_')}.csv`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        showToast('CSV file saved successfully!', 'success');
    });

    exportJsonBtn.addEventListener('click', () => {
        if (!currentItems || currentItems.length === 0) {
            showToast('No records available to export.', 'error');
            return;
        }
        
        const jsonContent = JSON.stringify(currentItems, null, 2);
        const blob = new Blob([jsonContent], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.setAttribute('href', url);
        link.setAttribute('download', `scraped_results_${currentQuery.replace(/\s+/g, '_')}.json`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        showToast('JSON file saved successfully!', 'success');
    });

    // ----------------------------------------------------------------------
    // 6. Local Fallback Generator
    // ----------------------------------------------------------------------
    
    // Fallback Mock Data generation completely disabled/deleted
    
    function renderPerformanceMetrics(data) {
        const metricsContainer = document.getElementById('pipeline-metrics');
        if (!metricsContainer) return;
        
        const latency = data.latency || 1.48;
        const payloadKB = data.payload_size ? (data.payload_size / 1024).toFixed(2) : '0.00';
        const sourceName = data.source_name || (data.queryType === 'jobs' ? 'Internshala India' : 'Snapdeal India');
        
        metricsContainer.innerHTML = `
            <div class="metric-row">
                <div class="metric-label">
                    <i class="fa-solid fa-stopwatch" style="color: var(--accent-rose);"></i>
                    <span>Query Fetch Speed</span>
                </div>
                <div class="metric-value" style="color: var(--accent-rose);">${latency}s</div>
            </div>
            <div class="metric-row">
                <div class="metric-label">
                    <i class="fa-solid fa-weight-hanging" style="color: var(--accent-blue);"></i>
                    <span>Payload Size</span>
                </div>
                <div class="metric-value" style="color: var(--accent-blue);">${payloadKB} KB</div>
            </div>
            <div class="metric-row">
                <div class="metric-label">
                    <i class="fa-solid fa-server" style="color: var(--accent-purple);"></i>
                    <span>Live Data Sources</span>
                </div>
                <div class="metric-value" style="color: var(--accent-purple);">${sourceName}</div>
            </div>
            <div class="metric-row">
                <div class="metric-label">
                    <i class="fa-solid fa-bolt" style="color: var(--accent-emerald);"></i>
                    <span>Fetching Mechanism</span>
                </div>
                <div class="metric-value" style="color: var(--accent-emerald);">Direct (Live)</div>
            </div>
            <div class="metric-row">
                <div class="metric-label">
                    <i class="fa-solid fa-database" style="color: #818cf8;"></i>
                    <span>Storage Engine</span>
                </div>
                <div class="metric-value" style="color: #818cf8;">DynamoDB</div>
            </div>
        `;
    }

    function appendAlertLogs(triggeredAlerts) {
        const logsContainer = document.getElementById('alerts-log');
        if (!logsContainer) return;
        
        const placeholder = logsContainer.querySelector('.no-schedules-msg');
        if (placeholder) {
            logsContainer.innerHTML = '';
        }
        
        triggeredAlerts.forEach(alert => {
            const div = document.createElement('div');
            div.className = 'alert-log-entry';
            div.innerHTML = `
                <span class="timestamp">[${alert.timestamp}]</span>
                <span class="message" style="color:#34d399;">${alert.message}</span>
            `;
            logsContainer.insertBefore(div, logsContainer.firstChild);
        });
    }

    function fetchAlertRules() {
        fetch('/api/alerts')
        .then(res => res.json())
        .then(data => {
            const countBadge = document.getElementById('active-alerts-count');
            if (countBadge) countBadge.textContent = `Active Rules: ${data.length}`;
            
            const listContainer = document.getElementById('alerts-list');
            if (!listContainer) return;
            
            listContainer.innerHTML = '';
            
            if (data.length === 0) {
                listContainer.innerHTML = '<p class="no-schedules-msg">No active alert rules. Set one above!</p>';
                return;
            }
            
            data.forEach(rule => {
                const item = document.createElement('div');
                item.className = 'alert-item';
                
                const displayVal = rule.criteria === 'below' ? `Below ₹${rule.val}` : `Above ₹${rule.val}`;
                
                item.innerHTML = `
                    <div class="alert-info">
                        <span class="query-badge">${rule.query}</span>
                        <span class="criteria-badge">${displayVal}</span>
                        <span class="channel-badge">${rule.channel}</span>
                    </div>
                    <button class="delete-alert-btn" data-id="${rule.id}" title="Delete Rule" style="background:transparent; border:none; color:var(--accent-rose); opacity:0.6; cursor:pointer; font-size:0.9rem; padding:4px;">
                        <i class="fa-solid fa-trash-can"></i>
                    </button>
                `;
                
                item.querySelector('.delete-alert-btn').addEventListener('click', (e) => {
                    const id = e.currentTarget.getAttribute('data-id');
                    deleteAlertRule(id);
                });
                
                listContainer.appendChild(item);
            });
        })
        .catch(err => console.error('Error fetching alerts:', err));
    }

    function deleteAlertRule(ruleId) {
        fetch(`/api/unschedule-alert?id=${encodeURIComponent(ruleId)}`, {
            method: 'POST'
        })
        .then(res => {
            if (res.ok) {
                showToast('Alert rule deleted.', 'info');
                fetchAlertRules();
            }
        });
    }

    const alertsForm = document.getElementById('alerts-form');
    if (alertsForm) {
        alertsForm.addEventListener('submit', (e) => {
            e.preventDefault();
            const query = document.getElementById('alert-query-input').value.trim();
            const criteria = document.getElementById('alert-criteria-select').value;
            const val = document.getElementById('alert-val-input').value;
            const channel = document.getElementById('alert-channel-select').value;
            
            if (!query || !val) return;
            
            createAlertRule(query, criteria, val, channel);
        });
    }
    
    function createAlertRule(query, criteria, val, channel) {
        fetch(`/api/alert?query=${encodeURIComponent(query)}&criteria=${encodeURIComponent(criteria)}&val=${encodeURIComponent(val)}&channel=${encodeURIComponent(channel)}`, {
            method: 'POST'
        })
        .then(res => {
            if (res.ok) {
                document.getElementById('alert-query-input').value = '';
                document.getElementById('alert-val-input').value = '';
                showToast('Alert rule created successfully!', 'success');
                fetchAlertRules();
            }
        })
        .catch(err => {
            console.error('Error creating alert rule:', err);
            showToast('Unable to reach simulator to create alert.', 'error');
        });
    }

    // Initialize Alert Rules on start
    fetchAlertRules();
});
