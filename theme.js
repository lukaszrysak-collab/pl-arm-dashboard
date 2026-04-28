(function () {
  var STORAGE_KEY = 'bolt-theme';

  function getPreferred() {
    var saved = localStorage.getItem(STORAGE_KEY);
    if (saved === 'light' || saved === 'dark') return saved;
    return 'dark';
  }

  function apply(theme) {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem(STORAGE_KEY, theme);
    var btns = document.querySelectorAll('.themeToggleBtnSync');
    for (var i = 0; i < btns.length; i++) {
      btns[i].innerHTML = theme === 'dark' ? SUN_SVG : MOON_SVG;
      btns[i].style.background = theme === 'dark' ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.03)';
    }
    var gate = document.getElementById('authGate');
    if (gate) gate.style.background = theme === 'light' ? '#f2f3f7' : '#0f0f1a';
  }

  var SUN_SVG = '<svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg>';
  var MOON_SVG = '<svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>';

  var IDS = ['#guideModal','#helpModal','#wbrGuideModal'];
  var L = IDS.map(function(s){return '[data-theme="light"] '+s});
  function S(suffix){return L.map(function(s){return s+suffix}).join(',')}
  var C = S(' > div');
  var r = [
    C+'{background:#fff !important;border-color:#dfe1e8 !important;color:#1a1a2e !important;'+
      'box-shadow:0 24px 80px rgba(0,0,0,0.25),0 0 0 1px rgba(0,0,0,0.05) !important;scrollbar-color:rgba(0,0,0,0.15) transparent !important}',
    S(' h2')+'{color:#1a1a2e !important}',
    S(' h3')+'{color:#7c3aed !important}',
    S(' [style*="color:#f0f0f5"]')+'{color:#1a1a2e !important}',
    S(' [style*="color:#c0c0d0"]')+'{color:#3a3a50 !important}',
    S(' [style*="color:#6b7280"]')+'{color:#71717a !important}',
    S(' [style*="color:#6b6b80"]')+'{color:#71717a !important}',
    S(' [style*="color:#a0a0b8"]')+'{color:#4a4a5e !important}',
    S(' [style*="color:#a78bfa"]')+'{color:#7c3aed !important}',
    S(' [style*="color:#c4b5fd"]')+'{color:#7c3aed !important}',
    S(' [style*="color:#8b5cf6"]')+'{color:#6d28d9 !important}',
    S(' [style*="color:#fff"]')+'{color:#fff !important}',
    S(' b')+'{color:inherit}',
    S(' strong')+'{color:inherit}',
    S(' [style*="solid #1e1e30"]')+'{border-color:#e8e8f0 !important;border-bottom-color:#e8e8f0 !important}',
    S(' [style*="solid #2a2a40"]')+'{border-color:#dfe1e8 !important;border-bottom-color:#dfe1e8 !important}',
    S(' [style*="solid rgba(42,42,64"]')+'{border-bottom-color:#e8e8f0 !important;border-color:#e8e8f0 !important}',
    S(' tr[style*="border-bottom"]')+'{border-bottom-color:#e8e8f0 !important}',
    S(' th[style]')+'{border-bottom-color:#dfe1e8 !important}',
    S(' [style*="background:#1e1e30"]')+'{background:#f0f0f5 !important}',
    S(' [style*="background:#12121e"]')+'{background:#fff !important}',
    S(' code[style*="background:#1e1e30"]')+'{background:#f0f0f5 !important;color:#7c3aed !important}',
    S(' kbd')+'{background:#e8e8f0 !important;color:#3a3a50 !important}',
    S('')+'{background:rgba(0,0,0,0.6) !important;backdrop-filter:blur(8px) !important;-webkit-backdrop-filter:blur(8px) !important}',
    S(' button[style*="rgba(255,255,255,0.06)"]')+'{background:rgba(0,0,0,0.05) !important;border-color:#dfe1e8 !important;color:#71717a !important}',
    S(' button[style*="rgba(255,255,255,0.12)"]')+'{border-color:#dfe1e8 !important}',
    S(' div[style*="border:1px solid #2a2a40"]')+'{border-color:#dfe1e8 !important}',
    S(' button[style*="color:#6b7280"]')+'{color:#71717a !important}',
    C+'::-webkit-scrollbar-thumb{background:rgba(0,0,0,0.15) !important}',
    C+'::-webkit-scrollbar-track{background:transparent !important}'
  ];
  var lightCSS = document.createElement('style');
  lightCSS.textContent = r.join('\n');
  document.head.appendChild(lightCSS);

  apply(getPreferred());

  function createToggleBtn(size) {
    var btn = document.createElement('button');
    btn.className = 'themeToggleBtnSync';
    btn.title = 'Toggle light / dark theme';
    var isDark = getPreferred() === 'dark';
    var w = size || 38;
    btn.setAttribute('style',
      'display:inline-flex;align-items:center;justify-content:center;width:'+w+'px;height:'+w+'px;' +
      'border-radius:8px;background:' + (isDark ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.03)') + ';' +
      'border:1px solid var(--border,#dfe1e8);' +
      'color:var(--text-secondary,#a0a0b8);cursor:pointer;transition:all .15s;padding:0;flex-shrink:0');
    btn.innerHTML = getPreferred() === 'dark' ? SUN_SVG : MOON_SVG;
    btn.addEventListener('mouseover', function () {
      this.style.background = 'rgba(139,92,246,0.08)';
      this.style.color = '#c4b5fd';
      this.style.borderColor = 'rgba(139,92,246,0.25)';
    });
    btn.addEventListener('mouseout', function () {
      var isDark = document.documentElement.getAttribute('data-theme') !== 'light';
      this.style.background = isDark ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.03)';
      this.style.color = '';
      this.style.borderColor = '';
    });
    btn.addEventListener('click', function () {
      var next = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
      apply(next);
    });
    return btn;
  }

  function injectButtons() {
    var sidebarWrap = document.getElementById('themeToggleWrap');
    if (sidebarWrap && !sidebarWrap.querySelector('.themeToggleBtnSync')) {
      sidebarWrap.appendChild(createToggleBtn(38));
    }
    var topBarWrap = document.getElementById('themeToggleWrapTopBar');
    if (topBarWrap && !topBarWrap.querySelector('.themeToggleBtnSync')) {
      topBarWrap.appendChild(createToggleBtn(30));
    }
    var topBarLgWrap = document.getElementById('themeToggleWrapTopBarLg');
    if (topBarLgWrap && !topBarLgWrap.querySelector('.themeToggleBtnSync')) {
      topBarLgWrap.appendChild(createToggleBtn(38));
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', injectButtons);
  } else {
    injectButtons();
  }
})();
